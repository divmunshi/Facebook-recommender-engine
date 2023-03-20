# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Install required packages
RUN apt-get update \
    && apt-get install -y gnupg2 curl postgresql-client redis-tools \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    && ACCEPT_EULA=Y apt-get install -y mssql-tools \
    && echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc \
    && /bin/bash -c "source ~/.bashrc" \
    && pip install --upgrade pip \
    && pip install torch matplotlib scikit-learn numpy pandas fastapi jupyter \
    && pip install docker apache-airflow \
    && curl -L "https://github.com/prometheus/prometheus/releases/download/v2.33.0/prometheus-2.33.0.linux-amd64.tar.gz" | tar -xz -C /tmp \
    && mv /tmp/prometheus-2.33.0.linux-amd64 /prometheus

# Install AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install

# Install Apache Kafka
RUN curl "https://downloads.apache.org/kafka/3.0.0/kafka_2.13-3.0.0.tgz" -o "kafka.tgz" \
    && tar -xzf kafka.tgz \
    && mv kafka_2.13-3.0.0 /kafka

# Install Grafana
RUN apt-get install -y apt-transport-https software-properties-common wget \
    && wget -q -O - https://packages.grafana.com/gpg.key | apt-key add - \
    && add-apt-repository "deb https://packages.grafana.com/oss/deb stable main" \
    && apt-get update \
    && apt-get install -y grafana

# Set environment variables
ENV KAFKA_HOME="/kafka"
ENV PATH="$PATH:$KAFKA_HOME/bin"
ENV PROMETHEUS_HOME="/prometheus"
ENV PATH="$PATH:$PROMETHEUS_HOME"

# Expose required ports
EXPOSE 80 9092 3000 9090

# Start the Jupyter server
CMD ["jupyter", "notebook", "--ip", "0.0.0.0", "--port", "80", "--no-browser"]
