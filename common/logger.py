import logging
from elasticsearch import Elasticsearch
from datetime import datetime


class Logger:
    _logger = None

    @classmethod
    def get_logger(cls, name="app", es_host="http://elasticsearch:9200", index="app_logs", level=logging.DEBUG):
        if cls._logger:
            return cls._logger

        logger = logging.getLogger(name)
        logger.setLevel(level)

        if not logger.handlers:
            es = Elasticsearch(es_host)

            class ESHandler(logging.Handler):
                def emit(self, record):
                    try:
                        es.index(index=index, document={
                            "timestamp": datetime.utcnow().isoformat(),
                            "level": record.levelname,
                            "logger": record.name,
                            "message": record.getMessage()
                        })
                    except Exception as e:
                        print(f"ES log failed: {e}")

            logger.addHandler(ESHandler())
            logger.addHandler(logging.StreamHandler())

        cls._logger = logger
        return logger
