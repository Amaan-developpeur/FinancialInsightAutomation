# Financial_Insights/airflow-docker/dags/financial_insights_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import sys
import os

# --------- Add scripts folder to path ----------
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR / "scripts"))

# --------- Default args ----------
default_args = {
    "owner": "Amaan",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 26),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --------- Helper to disable OTEL (metrics spam) ----------
def disable_otel():
    os.environ["OTEL_METRICS_EXPORTER"] = "none"
    os.environ["OTEL_TRACES_EXPORTER"] = "none"


# --------- DAG ----------
with DAG(
    "financial_insights_pipeline",
    default_args=default_args,
    description="Full Financial Insights RAG pipeline",
    schedule_interval="@hourly",  # change to "@daily" if needed
    catchup=False,
) as dag:

    # --------- Step 1: Data ingestion ----------
    def run_ingestion_task():
        from ingestion import run_ingestion
        run_ingestion()

    ingestion_task = PythonOperator(
        task_id="data_ingestion",
        python_callable=run_ingestion_task
    )

    # --------- Step 2: Chunking ----------
    def run_chunking_task():
        from chunking import run_chunking
        run_chunking()

    chunking_task = PythonOperator(
        task_id="chunking",
        python_callable=run_chunking_task
    )

    # --------- Step 3: Embeddings ----------
    def run_embeddings_task():
        disable_otel()
        from embeddings import run_embeddings
        run_embeddings()

    embeddings_task = PythonOperator(
        task_id="create_embeddings",
        python_callable=run_embeddings_task
    )

    # --------- Step 4: Test RAG QA ----------
    def sample_rag_query_task():
        disable_otel()
        from rag_query import run_query
        query = "What are the recent developments in the Indian banking sector?"
        answer, sources = run_query(query)
        print("Answer:", answer)
        print("Sources:", sources)

    rag_task = PythonOperator(
        task_id="test_rag_query",
        python_callable=sample_rag_query_task
    )

    # --------- Task Dependencies ----------
    ingestion_task >> chunking_task >> embeddings_task >> rag_task
