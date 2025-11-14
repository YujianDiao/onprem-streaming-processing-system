FROM apache/superset:3.0.0

USER root

# Install database drivers and additional dependencies
RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.9 \
    pymysql==1.1.0 \
    pyiceberg==0.5.1 \
    sqlalchemy-trino==0.5.0 \
    pyhive==0.7.0 \
    thrift==0.16.0 \
    sasl==0.3.1 \
    thrift-sasl==0.4.3

# Switch back to superset user
USER superset

# Initialize the database
RUN superset db upgrade && \
    superset init

# Set the default command
CMD ["/usr/bin/run-server.sh"]
