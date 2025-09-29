# scripts/chunking.py

import os
import json
import re
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
CHUNK_DIR = BASE_DIR / "data" / "chunks"
CHUNK_DIR.mkdir(parents=True, exist_ok=True)


# --------- Text Cleaning ----------
def clean_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"<.*?>", "", text)      # remove HTML tags
    text = re.sub(r"\s+", " ", text)       # remove multiple spaces
    return text.strip()


# --------- Text Chunking ----------
def chunk_text(text: str, chunk_size=300, overlap=50) -> list:
    words = text.split()
    chunks = []
    for i in range(0, len(words), chunk_size - overlap):
        chunk = words[i:i + chunk_size]
        if len(chunk) < 30:
            continue
        chunks.append(" ".join(chunk))
    return chunks


# --------- Process single news JSON file ----------
def process_news_file(news_json_path: str):
    with open(news_json_path, "r", encoding="utf8") as f:
        data = json.load(f)

    articles = data.get("articles", [])
    chunk_records = []

    for art in articles:
        source = art.get("source", {}).get("name")
        title = clean_text(art.get("title", ""))
        content = clean_text(art.get("content", "")) or clean_text(art.get("description", ""))
        if not content:
            continue

        chunks = chunk_text(content)
        for i, ch in enumerate(chunks):
            chunk_records.append({
                "source": source,
                "title": title,
                "publishedAt": art.get("publishedAt"),
                "chunk_id": f"{title[:30]}_{i}",
                "text": ch,
                "raw_file": str(news_json_path)
            })

    if chunk_records:
        df = pd.DataFrame(chunk_records)
        ts = os.path.basename(news_json_path).replace(".json", "")
        out_path = CHUNK_DIR / f"chunks_{ts}.jsonl"
        df.to_json(out_path, orient="records", lines=True, force_ascii=False)
        print(f"Saved chunks → {out_path}")


# --------- Main pipeline ----------
def run_chunking():
    news_files = [x for x in os.listdir(RAW_DIR) if x.endswith(".json")]
    for nf in news_files:
        full_path = RAW_DIR / nf
        process_news_file(full_path)


if __name__ == "__main__":
    run_chunking()
