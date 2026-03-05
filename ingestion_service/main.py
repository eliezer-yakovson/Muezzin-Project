import os
import json
import logging
from pathlib import Path
from fastapi import FastAPI
from confluent_kafka import Producer

app = FastAPI()

logging.basicConfig(level=logging.INFO)


producer = Producer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
})

def produce(topic: str, value: dict):
    producer.produce(
        topic,
        value=json.dumps(value).encode("utf-8")
    )
    producer.flush()

@app.get("/scan")
def scan_directory():
    directory = os.getenv("FILES_DIRECTORY", "/data")
    dir_path = Path(directory)
    
    if not dir_path.exists() or not dir_path.is_dir():
        logging.error(f"Directory {directory} does not exist")
        return {"message": f"Directory {directory} does not exist", "results": []}
    
    results = []
    
    for filepath in dir_path.iterdir():
        if not filepath.is_file():
            continue

        try:
            stat = filepath.stat()
            
            try:
                creation_time = stat.st_birthtime
            except AttributeError:
                creation_time = stat.st_ctime
                
            metadata = {
                "file_name": filepath.name,
                "file_size_bytes": stat.st_size,
                "creation_date": creation_time,
                "extension": filepath.suffix
            }
            
            payload = {
                "file_path": str(filepath),
                "metadata": metadata
            }
            
            produce("files_metadata", payload)
            results.append(payload["file_path"])
            
        except Exception as e:
            logging.error(f"Error processing file {filepath.name}: {e}")
            continue

    return {"message": "Directory scanned successfully", "results": results}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)