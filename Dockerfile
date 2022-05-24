FROM apache/airflow:2.2.4
USER root
RUN apt update
RUN apt search openjdk
RUN apt install openjdk-11-jdk -y
COPY requirements.txt .
RUN pip install -r requirements.txt