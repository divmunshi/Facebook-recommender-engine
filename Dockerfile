FROM python:3.9-slim-buster


# Install netcat
RUN apt-get update && apt-get install -y netcat
RUN pip install -U flask
RUN pip install kafka-python
# RUN pip install confluent-kafka

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

EXPOSE 8080

CMD ["python", "wait-for-kafka.py", "kafka:9092", "60", "--", "flask", "run", "--host=0.0.0.0", "--port=8080"]


