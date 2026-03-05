FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    flac \
    && rm -rf /var/lib/apt/lists/*

COPY stt_service/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ ./common/
COPY stt_service/main.py ./

CMD ["python", "main.py"]
