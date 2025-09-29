# airflow-docker/Dockerfile
FROM apache/airflow:2.7.3-python3.10

# Switch to airflow user before installing packages
USER airflow

# Copy requirements
COPY scripts/requirements.txt /tmp/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Switch back to airflow user explicitly (optional)
USER airflow
