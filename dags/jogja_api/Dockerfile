FROM python:3.11.9

RUN mkdir /code

WORKDIR /code

COPY /dags/jogja_api/.env /code/.env

COPY /dags/requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir  -r /code/requirements.txt

COPY /dags/jogja_api/kafka_consume_stream.py /code/kafka_consume_stream.py

CMD [ "python3", "kafka_consume_stream.py" ]