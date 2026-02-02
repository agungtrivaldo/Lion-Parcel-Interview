# Dockerfile.airflow
FROM apache/airflow:2.9.1

USER root

# Install dependencies untuk Postgres & Python
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy DAGs dan scripts
COPY airflow/dags /opt/airflow/dags

# Set environment variables
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__CORE__FERNET_KEY=IDuTBGUhBX87tgM9ez-3bQXk8ciMOJPnRhgSf5c_HIs=
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow_user:airflow_pass@postgres_airflow:5432/airflow_db

# Expose Airflow Webserver port
EXPOSE 8080

# Default command
ENTRYPOINT ["bash", "-c"]
CMD ["airflow db init && airflow users create --username admin --password admin --firstname admin --lastname admin --role Admin --email admin@example.com && airflow webserver"]
