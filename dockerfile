FROM apache/airflow:latest
FROM apache/airflow:2.9.1-python3.12

USER root
RUN apt-get update && \
    apt-get -y install git && \
    apt-get clean

USER airflow
RUN pip install yfinance pandas fastapi 