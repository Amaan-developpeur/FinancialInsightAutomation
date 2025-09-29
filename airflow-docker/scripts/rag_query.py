# airflow-docker/scripts/rag_query.py

from pathlib import Path
from langchain_community.llms import Ollama
from langchain_community.vectorstores import Chroma
from langchain.chains import RetrievalQA

# Import embedding model from embeddings.py in same package
from airflow_docker.scripts.embeddings import embedding_model  

# --------- Paths ----------
# Go up two levels to project root (FinancialInsights/)
BASE_DIR = Path(__file__).resolve().parents[2]
CHROMA_DIR = BASE_DIR / "chroma_db"

# --------- Load vectorstore ----------
def load_vectorstore():
    vectordb = Chroma(
        persist_directory=str(CHROMA_DIR),
        embedding_function=embedding_model
    )
    return vectordb

# --------- Set up retriever ----------
def get_retriever(vectordb, top_k=3):
    return vectordb.as_retriever(search_kwargs={"k": top_k})

# --------- Build RetrievalQA chain ----------
def build_qa_chain(retriever, model_name="mistral"):
    llm = Ollama(model=model_name)
    qa_chain = RetrievalQA.from_chain_type(
        llm=llm,
        retriever=retriever,
        return_source_documents=True
    )
    return qa_chain

# --------- Run a single query ----------
def run_query(query: str):
    vectordb = load_vectorstore()
    retriever = get_retriever(vectordb)
    qa_chain = build_qa_chain(retriever)
    
    response = qa_chain.invoke(query)
    
    answer = response["result"]
    sources = [doc.metadata for doc in response["source_documents"]]
    
    return answer, sources

# --------- Example usage ----------
if __name__ == "__main__":
    query = "What are the recent developments in the Indian banking sector?"
    answer, sources = run_query(query)
    
    print("Answer:\n", answer)
    print("\nSources:")
    for s in sources:
        print("-", s)
