FROM apache/airflow:3.0.6-python3.10

USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /opt/native/jars \
    && curl -fsSL https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.3.1/iceberg-spark-runtime-3.3_2.12-1.3.1.jar -o /opt/native/jars/iceberg.jar \
    && curl -fsSL https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar -o /opt/native/jars/hadoop-aws.jar \
    && curl -fsSL https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar -o /opt/native/jars/aws-sdk.jar
USER airflow
RUN pip install --no-cache-dir 'apache-airflow==3.0.6' 'pyspark==3.3.2' 'minio==7.2.20' \
    'numpy==1.24.3' 'pandas==1.5.3' 'pyarrow==12.0.1'
WORKDIR /opt/native
COPY runtime.py scenarios.py fixtures.py run.json ./
COPY scripts ./scripts
ENTRYPOINT ["python", "-B", "/opt/native/runtime.py"]
