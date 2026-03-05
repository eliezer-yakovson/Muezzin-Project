import os
import json
import hashlib
from confluent_kafka import Consumer
from pymongo import MongoClient
from gridfs import GridFS
from elasticsearch import Elasticsearch
from common.logger import Logger

logging = Logger.get_logger(
    name="data_transfer",
    es_host=os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200"),
    index="app_logs"
)

# MongoDB 
client = MongoClient(os.getenv("MONGODB_URI", "mongodb://mongo:27017"))
db = client[os.getenv("MONGODB_DB", "podcast_db")]
gfs = GridFS(db)

# Elasticsearch 
es = Elasticsearch([os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200")])

# Kafka  
consumer = Consumer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    "group.id": os.getenv("KAFKA_GROUP_ID", "data_transfer_group"),
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["files_metadata"])

def calculate_unique_id(file_path: str) -> str:
    hasher = hashlib.sha256()
    with open(file_path, 'rb') as f:
        buf = f.read()
        hasher.update(buf)
    return hasher.hexdigest()

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
        
        unique_id = calculate_unique_id(file_path)
        
        if not gfs.exists({"_id": unique_id}):
            with open(file_path, "rb") as file_content:
                gfs.put(file_content, _id=unique_id, filename=metadata["file_name"])
            logging.info(f"File {metadata['file_name']} uploaded to MongoDB.")
        else:
            logging.info(f"File {metadata['file_name']} already exists in MongoDB.")
            
        metadata["unique_id"] = unique_id
        es.index(index="podcasts_metadata", id=unique_id, document=metadata)
        logging.info(f"Metadata for {metadata['file_name']} indexed in Elasticsearch.")
        
    except Exception as e:
        logging.error(f"Error processing message: {e}")