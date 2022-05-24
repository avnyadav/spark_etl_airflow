FROM apache/airflow:2.2.4
USER root
RUN apt update
RUN apt install openjdk-11-jdk -y
RUN echo "JAVA_HOME=$(readlink -f /usr/bin/javac | sed "s:/bin/javac::")" | tee -a ~/.bashrc
COPY requirements.txt .
RUN pip install -r requirements.txt