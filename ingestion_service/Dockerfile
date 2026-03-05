FROM python:3.10-slim

WORKDIR /app

COPY ingestion_service/requirements.txt ./
RUN apt-get update && apt-get install -y tesseract-ocr
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ ./common/
COPY ingestion_service/main.py ./

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
