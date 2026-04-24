FROM apache/airflow:2.6.3

USER root

# Cài Java + tool cần cho Spark
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk procps && \
    apt-get clean

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Cài pyspark
RUN pip install --no-cache-dir pyspark