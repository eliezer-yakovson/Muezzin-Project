FROM python:3.10-slim

WORKDIR /app

COPY data_transfer/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ ./common/
COPY data_transfer/main.py ./

EXPOSE 8001

CMD ["python", "main.py"]
