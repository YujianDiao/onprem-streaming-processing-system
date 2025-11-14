FROM python:3.11-slim

WORKDIR /app

# Install required packages
RUN pip install --no-cache-dir \
    kafka-python \
    faker \
    avro-python3

# Copy the generator script
COPY generator.py .

# Run the generator
CMD ["python", "generator.py"]
