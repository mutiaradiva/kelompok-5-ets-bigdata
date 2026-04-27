FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY producer_api.py ./producer_api.py

CMD ["python", "producer_api.py"]