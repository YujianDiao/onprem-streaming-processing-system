FROM apache/superset:latest

# Switch to root to install packages
USER root

# Install Trino packages into the system Python dist-packages
# The venv will inherit these via site-packages
RUN pip install --target=/usr/local/lib/python3.10/dist-packages \
    --no-cache-dir trino sqlalchemy-trino

# Install into the venv site-packages directly by copying
RUN cp -r /usr/local/lib/python3.10/dist-packages/trino* /app/.venv/lib/python3.10/site-packages/ && \
    cp -r /usr/local/lib/python3.10/dist-packages/sqlalchemy_trino* /app/.venv/lib/python3.10/site-packages/ && \
    find /usr/local/lib/python3.10/dist-packages/ -maxdepth 1 -type d \( -name "*trino*" -o -name "*requests*" -o -name "*certifi*" -o -name "*urllib3*" -o -name "*charset*" -o -name "*idna*" -o -name "*pytz*" -o -name "*dateutil*" -o -name "*tzlocal*" -o -name "*orjson*" -o -name "*lz4*" -o -name "*zstandard*" \) -exec cp -r {} /app/.venv/lib/python3.10/site-packages/ \;

USER superset
