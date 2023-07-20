import json
import logging
from kafka import KafkaProducer

class UUIDFilter(logging.Filter):
    """
    Filter for logger
    Set UUID to default value of -1 if not specified
    UUID -1 logs are not specific to a job
    """
    def filter(self, record):
        if not hasattr(record, 'uuid'):
            record.uuid = -1
        return True

class KafkaHandler(logging.Handler):
    """
    Logging handler to write logs to a kafka topic
    """
    def __init__(self, logs_topic, bootstrap_servers):
        super().__init__()
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.topic = logs_topic

    def emit(self, record):
        """Emit the provided record to the kafka_client producer."""
        # drop kafka logging to avoid infinite recursion
        if 'kafka.' in record.name:
            return

        try:
            # apply the logger formatter
            msg = self.format(record)
            self.producer.send(self.topic, msg)
            self.flush(timeout=1.0)
        except Exception:
            logging.Handler.handleError(self, record)

    def flush(self, timeout=None):
        """Flush the objects"""
        self.producer.flush(timeout=timeout)

    def close(self):
        """Close the producer and clean up"""
        self.acquire()
        try:
            if self.producer:
                self.producer.close()

            logging.Handler.close(self)
        finally:
            self.release()

def configure_logger(microservice: str, logs_topic: str, bootstrap_servers: list, log_to_console: bool=True, level=logging.INFO):
    """
    Create and return a logger that logs to a kafka topic
    """
    logger = logging.getLogger(microservice)
    logger.setLevel(level)
    logger.addFilter(UUIDFilter())

    formatter = logging.Formatter('%(levelname)s;%(asctime)s;%(name)s;%(uuid)s;%(message)s')
    kafka_handler = KafkaHandler(logs_topic=logs_topic, bootstrap_servers=bootstrap_servers)
    kafka_handler.setFormatter(formatter)
    logger.addHandler(kafka_handler)

    if log_to_console:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger
