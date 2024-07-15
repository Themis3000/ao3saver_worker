FROM python:3.11-slim-buster

RUN mkdir app
WORKDIR /app

COPY . .

RUN pip3 install -r requirements.txt

CMD ["python", "./main.py"]