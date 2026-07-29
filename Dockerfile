# Use official lightweight Python image
FROM python:3.11-slim

# Install OpenJDK 17 (JRE) and other dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set Java environmental variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Set working directory
WORKDIR /app

# Copy dependency definition and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY app/ app/
COPY data/ data/
COPY mock_api.py .

# Expose ports for FastAPI (8000) and Mock API (9000)
EXPOSE 8000
EXPOSE 9000

# Start both mock API in the background and uvicorn FastAPI in the foreground
CMD python mock_api.py --host 0.0.0.0 --port 9000 & uvicorn app.main:app --host 0.0.0.0 --port 8000
