# airflow-docker/scripts/embeddings.py

import pandas as pd
from pathlib import Path
from langchain_community.vectorstores import Chroma
from langchain_huggingface import HuggingFaceEmbeddings

# --------- Directories ----------
BASE_DIR = Path(__file__).resolve().parent.parent.parent  # FinancialInsights/
DATA_DIR = BASE_DIR / "data"
CHUNK_DIR = DATA_DIR / "chunks"
CHROMA_DIR = Path("/tmp/chroma_db")
CHROMA_DIR.mkdir(parents=True, exist_ok=True)



# --------- Load chunked JSONL ----------
def load_chunks(jsonl_path: Path) -> pd.DataFrame:
    """Load chunked text data from JSONL into a DataFrame."""
    return pd.read_json(jsonl_path, lines=True)


# --------- Initialize embedding model ----------
embedding_model = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")


# --------- Create & persist Chroma vectorstore ----------
def create_chroma_vectorstore(df: pd.DataFrame, persist_dir: Path):
    """Create a Chroma vectorstore from chunk DataFrame and persist it."""
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
    """Generate embeddings for all chunked JSONL files."""
    chunk_files = sorted(CHUNK_DIR.glob("*.jsonl"))
    if not chunk_files:
        print(f"No chunk files found in {CHUNK_DIR}")
        return

    for cf in chunk_files:
        print(f"Processing {cf}...")
        df = load_chunks(cf)
        create_chroma_vectorstore(df, CHROMA_DIR)


if __name__ == "__main__":
    run_embeddings()
