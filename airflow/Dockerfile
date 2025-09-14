FROM apache/airflow:2.10.4-python3.11

USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=${JAVA_HOME}/bin:$PATH

USER airflow

# cài provider + pyspark
RUN pip install --no-cache-dir \
    pyspark==3.4.1 \
    apache-airflow-providers-apache-spark \
