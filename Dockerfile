FROM python:3.8

RUN apt update && apt -y install software-properties-common gcc
RUN git clone https://github.com/edenhill/librdkafka
RUN cd librdkafka && ./configure && make && make install && ldconfig

COPY requirements.txt requirements.txt
RUN pip3.8 install -r requirements.txt