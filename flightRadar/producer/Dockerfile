# producer/Dockerfile

FROM python:3.9-slim

EXPOSE 1129

COPY . ./producer

RUN pip install -r producer/requirements.txt

CMD ["python", "producer/app_producer.py"]
