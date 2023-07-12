FROM python:3.10-slim-bullseye

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip3 install --no-cache-dir -r requirements.txt

RUN python3 -m nltk.downloader stopwords

COPY . .

CMD [ "python3", "data-preprocessing.py" ]