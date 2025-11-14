FROM apache/airflow:2.7.3-python3.11

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    default-libmysqlclient-dev \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

# Copy custom plugins and configurations
COPY --chown=airflow:root plugins /opt/airflow/plugins
COPY --chown=airflow:root config /opt/airflow/config

WORKDIR /opt/airflow
