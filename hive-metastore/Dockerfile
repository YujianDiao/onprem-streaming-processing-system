FROM apache/hive:4.0.0

# Install PostgreSQL JDBC driver and client tools
USER root

# Install PostgreSQL client for pg_isready and wget for downloading JDBC driver
RUN apt-get update && \
    apt-get install -y postgresql-client wget && \
    rm -rf /var/lib/apt/lists/*

# Download PostgreSQL JDBC driver and S3A libraries
RUN wget -O /opt/hive/lib/postgresql-jdbc.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.1.jar && \
    chmod 644 /opt/hive/lib/postgresql-jdbc.jar && \
    wget -O /opt/hive/lib/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    chmod 644 /opt/hive/lib/hadoop-aws-3.3.4.jar && \
    wget -O /opt/hive/lib/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    chmod 644 /opt/hive/lib/aws-java-sdk-bundle-1.12.262.jar

# Copy Hadoop core-site.xml with S3 configuration
COPY core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
RUN chmod 644 /opt/hadoop/etc/hadoop/core-site.xml

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && \
    chown hive:hive /entrypoint.sh

# Switch back to hive user
USER hive

# Use custom entrypoint
ENTRYPOINT ["/entrypoint.sh"]
