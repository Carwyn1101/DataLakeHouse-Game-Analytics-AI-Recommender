# streamlit-ai/Dockerfile
# Dùng Debian Bookworm (stable) - có OpenJDK 17
FROM python:3.10-slim-bookworm

# Metadata
LABEL maintainer="Carwyn1101"
LABEL description="Game Recommendation AI - TLCN 2025"

# Install Java 17 & utilities
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Expose port
EXPOSE 8501

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl --fail http://localhost:8501/_stcore/health || exit 1

ENV STREAMLIT_SERVER_FILE_WATCHER_TYPE=none

# Run app
CMD ["streamlit", "run", "app.py", \
     "--server.address", "0.0.0.0", \
     "--server.port", "8501", \
     "--server.headless", "true", \
     "--browser.gatherUsageStats", "false"]

