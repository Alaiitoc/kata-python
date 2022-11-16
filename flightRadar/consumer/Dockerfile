# consumer/Dockerfile

FROM python:3.9-slim

EXPOSE 2000

COPY . ./consumer

RUN pip install -r consumer/requirements.txt

CMD ["python", "consumer/app_consumer.py"]