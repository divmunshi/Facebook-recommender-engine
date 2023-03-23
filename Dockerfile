FROM python:3.8

COPY requirements.txt requirements.txt
RUN pip3.8 install -r requirements.txt