FROM python:3.11-slim-bullseye

RUN pip install kafka-python

COPY . .

CMD ["python3", "-u", "topic-list.py"]