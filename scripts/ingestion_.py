# scripts/ingestion.py

import os
import json
import sqlite3
import datetime
from pathlib import Path
from dotenv import load_dotenv

import requests
import pandas as pd
import yfinance as yf


# --------- Load environment variables ----------
load_dotenv()
API_KEY = os.getenv("NEWSAPI_KEY")

# --------- Paths ----------
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
STOCK_DIR = BASE_DIR / "data" / "stock"
DB_PATH = BASE_DIR / "chroma_db.sqlite"

RAW_DIR.mkdir(parents=True, exist_ok=True)
STOCK_DIR.mkdir(parents=True, exist_ok=True)


# --------- DB setup ----------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Stock prices table
    create_stock_table = """
    CREATE TABLE IF NOT EXISTS stock_prices (
        Datetime DATETIME,
        Ticker TEXT,
        Open REAL,
        High REAL,
        Low REAL,
        Close REAL,
        Volume INTEGER
    );
    """
    cursor.execute(create_stock_table)

    # News metadata table
    create_news_table = """
    CREATE TABLE IF NOT EXISTS news_meta (
        source TEXT,
        title TEXT,
        publishedAt TEXT,
        raw_file TEXT
    );
    """
    cursor.execute(create_news_table)

    conn.commit()
    return conn


# --------- Fetch news ----------
def fetch_news():
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": "NSE OR BASE OR FinTech",
        "language": "en",
        "pageSize": 20,
        "sortBy": "publishedAt",
        "apiKey": API_KEY,
    }
    response = requests.get(url, params=params, timeout=30)
    data = response.json()

    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H")
    filename = RAW_DIR / f"news_{timestamp}.json"

    with open(filename, "w", encoding="utf8") as fp:
        json.dump(data, fp)

    return filename


# --------- Store news metadata ----------
def store_news_metadata(conn, news_json_path):
    with open(news_json_path, "r", encoding="utf8") as f:
        data = json.load(f)

    articles = data.get("articles", [])
    meta = [
        (
            a.get("source", {}).get("name"),
            a.get("title"),
            a.get("publishedAt"),
            str(news_json_path),
        )
        for a in articles
    ]

    df = pd.DataFrame(meta, columns=["source", "title", "publishedAt", "raw_file"])
    df.to_sql("news_meta", conn, if_exists="append", index=False)


# --------- Fetch stock data ----------
def fetch_stocks(tickers=None):
    if tickers is None:
        tickers = ["HDFCBANK.NS", "ICICIBANK.NS", "SBIN.NS"]

    full_df = pd.DataFrame()
    for t in tickers:
        ticker = yf.Ticker(t)
        df = ticker.history(period="7d", interval="1h")
        df.reset_index(inplace=True)
        df["Ticker"] = t
        full_df = pd.concat([full_df, df], ignore_index=True)

    return full_df


# --------- Store stock data in SQLite ----------
def store_stocks(conn, df):
    # Keep only the columns that exist in SQLite table
    df = df[["Datetime", "Ticker", "Open", "High", "Low", "Close", "Volume"]]
    df.to_sql("stock_prices", conn, if_exists="append", index=False)


# --------- Pipeline entry ----------
def run_ingestion():
    conn = init_db()
    news_file = fetch_news()
    store_news_metadata(conn, news_file)

    stock_df = fetch_stocks()
    store_stocks(conn, stock_df)

    conn.close()
    print("Data ingestion completed.")


if __name__ == "__main__":
    run_ingestion()
