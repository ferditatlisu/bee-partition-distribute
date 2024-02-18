FROM registry.trendyol.com/platform/base/image/python:3.11.1-slim

WORKDIR /app
COPY . /app

RUN pip3 install -r requirements.txt
CMD ["python3", "-u", "main.py"]
