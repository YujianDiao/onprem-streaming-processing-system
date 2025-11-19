FROM flink:1.18.0-scala_2.12-java11

# Install Maven for building jobs
USER root
RUN apt-get update && \
    apt-get install -y maven && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /opt/flink

# Copy configuration files
COPY conf/ /opt/flink/conf/

# Copy job source code and build file
COPY jobs/ /opt/flink-jobs/
COPY pom.xml /opt/flink-jobs/

# Download additional dependencies (will be cached in Docker layer)
RUN cd /opt/flink-jobs && \
    mvn dependency:go-offline || true

# Create directory for compiled jobs
RUN mkdir -p /opt/flink/usrlib

# Switch back to flink user
USER flink
