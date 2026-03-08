import os
import json
import hashlib
import speech_recognition as sr
from confluent_kafka import Consumer
from elasticsearch import Elasticsearch
from common.bds_analyzer import analyze_transcription
from common.logger import Logger

logging = Logger.get_logger(
    name="stt_service",
    es_host=os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200"),
    index="app_logs"
)

es = Elasticsearch([os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200")])

consumer = Consumer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    "group.id": "stt_group",
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["files_metadata"])

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        logging.error(f"Consumer error: {msg.error()}")
        continue

    try:
        payload = json.loads(msg.value().decode("utf-8"))
        file_path = payload["file_path"]
        metadata = payload["metadata"]

        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            hasher.update(f.read())
        unique_id = hasher.hexdigest()

        recognizer = sr.Recognizer()
        with sr.AudioFile(file_path) as source:
            audio = recognizer.record(source)
        transcription = recognizer.recognize_google(audio)
        analysis_result = analyze_transcription(transcription)
        logging.info(f"Transcription for {metadata['file_name']}: {transcription}")

        es.update(
            index="podcasts_metadata",
            id=unique_id,
            body={
                "doc": {
                    "transcription": transcription,
                    "bds_percent": analysis_result["bds_percent"],
                    "is_bds": analysis_result["is_bds"],
                    "bds_threat_level": analysis_result["bds_threat_level"],
                    "bds_matches": analysis_result["bds_matches"],
                },
                "doc_as_upsert": True,
            }
        )
        logging.info(f"Updated ES document {unique_id} with transcription.")

    except Exception as e:
        logging.error(f"Error processing STT for message: {e}")