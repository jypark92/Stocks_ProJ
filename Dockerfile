FROM apache/airflow:2.5.1

USER root

# Install Java
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        g++ \
        openjdk-11-jdk \
        python3-dev \
        python3-pip \
        curl && \
    pip install pandas aiohttp pyarrow konlpy yfinance pymysql

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64    
USER airflow