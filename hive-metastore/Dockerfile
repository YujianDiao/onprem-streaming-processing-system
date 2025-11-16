FROM apache/hive:4.0.0

# Install PostgreSQL JDBC driver and client tools
USER root

# Install PostgreSQL client for pg_isready and wget for downloading JDBC driver
RUN apt-get update && \
    apt-get install -y postgresql-client wget && \
    rm -rf /var/lib/apt/lists/*

# Download PostgreSQL JDBC driver
RUN wget -O /opt/hive/lib/postgresql-jdbc.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.1.jar && \
    chmod 644 /opt/hive/lib/postgresql-jdbc.jar

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && \
    chown hive:hive /entrypoint.sh

# Switch back to hive user
USER hive

# Use custom entrypoint
ENTRYPOINT ["/entrypoint.sh"]
