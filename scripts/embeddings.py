# scripts/embeddings.py

import pandas as pd
from pathlib import Path
from langchain.vectorstores import Chroma
from langchain.embeddings import HuggingFaceEmbeddings

BASE_DIR = Path(__file__).resolve().parent.parent
CHUNK_DIR = BASE_DIR / "data" / "chunks"
CHROMA_DIR = BASE_DIR / "chroma_db"
CHROMA_DIR.mkdir(parents=True, exist_ok=True)


# --------- Load chunked JSONL ----------
def load_chunks(jsonl_path: Path) -> pd.DataFrame:
    return pd.read_json(jsonl_path, lines=True)


# --------- Initialize embedding model ----------
embedding_model = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")


# --------- Create & persist Chroma vectorstore ----------
def create_chroma_vectorstore(df: pd.DataFrame, persist_dir: Path):
    texts = df["text"].tolist()
    metadatas = df.drop(columns=["text"]).to_dict(orient="records")

    vectordb = Chroma.from_texts(
        texts=texts,
        embedding=embedding_model,
        metadatas=metadatas,
        persist_directory=str(persist_dir)
    )
    vectordb.persist()
    print(f"Saved vectorstore at {persist_dir}")


# --------- Main pipeline ----------
def run_embeddings():
    chunk_files = sorted(CHUNK_DIR.glob("*.jsonl"))
    for cf in chunk_files:
        df = load_chunks(cf)
        create_chroma_vectorstore(df, CHROMA_DIR)


if __name__ == "__main__":
    run_embeddings()
