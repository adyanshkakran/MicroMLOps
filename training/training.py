"""
Template for microservices
Reads config from an input topic, 
performs operations and then
produces message on output topic
"""

import os
import json
import time
import logging
import numpy as np
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
from pprint import pprint

from kafka_logger import configure_logger

from svm import svm
from random_forest import random_forest

load_dotenv(override=True) # env file has higher preference

"""
configuring kafka
these must be set in env
default input topic is filename (without .py)
output topic must be specified
"""
input_topic:str = os.environ.get("INPUT_TOPIC", os.path.basename(__file__)[:-3])
output_topic:str = os.environ.get("OUTPUT_TOPIC", "default_output_topic")
logs_topic:str = os.environ.get("LOGS_TOPIC", "logs")
kafka_broker:str = os.environ.get("KAFKA_BROKER", "localhost:9092")
consumer_group_id:str = os.environ.get("KCON_GROUP_ID", "default_group_id")
debug_mode:bool = os.environ.get("MICROML_DEBUG", "0") == "1"

#  wait for kafka to start
time.sleep(20)
logger = configure_logger(input_topic, logs_topic, [kafka_broker], level=logging.DEBUG if debug_mode else logging.INFO)
logger.info("done waiting for kafka")

if os.environ.get("MICROML_DEBUG", "0"):
    logger.debug(f"Input Topic: {input_topic}; Output Topic: {output_topic}")
    logger.debug(f"Group ID: {consumer_group_id}; Kafka Broker: {kafka_broker}")

def setup_kafka_consumer():
    """
    Create and return a kafka consumer
    Uses global variables for config
    Uses specified input_topic, group_id, kafka_broker
    """
    config = {
        "group_id": consumer_group_id,
        "bootstrap_servers": kafka_broker,
        "auto_offset_reset": "earliest"
    }
    try:
        consumer = KafkaConsumer(input_topic, **config)
        return consumer
    except Exception as e:
        raise Exception("Failed to create Kafka Consumer")


def setup_kafka_producer():
    """
    Create and return a kafka producer
    Uses global variables for config
    Uses specified kafka_broker as bootstrap_server
    """
    config = {
        "bootstrap_servers": kafka_broker
    }
    try:
        producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'), **config)
        return producer
    except Exception as e:
        raise Exception("Failed to create Kafka Producer")


#######
def read_and_execute(consumer: KafkaConsumer, producer: KafkaProducer):
    """
    Consumer listens for messages from input queue
    When it reads a message, processes it 
    Then producer sends output 
    """
    try:
        for message in consumer:
            output_message = process_message(message)
            send_message(output_message, producer)
    except KeyboardInterrupt:
        print("Exiting...")

def process_message(message):
    """
    Read incoming message
    Do necessary computation
    Return output message
    """
    message_obj = json.loads(message.value)
    if os.environ.get("MICROML_DEBUG", "0"):
        pprint(message_obj)
    
    model_config = message_obj["model"]
    training_config = message_obj["training"]
    data = pd.read_csv(message_obj["data"])
    job_uuid = message_obj["uuid"]

    if not model_config or not training_config:
        raise Exception("Model or training config not specified")
    
    execute(data, model_config, message_obj, job_uuid)
    message_obj["data"] = message_obj["data"][:-7] + ".csv"

    return message_obj

def execute(data: pd.DataFrame, model_config: str, config: dict, job_uuid: str):
    try:
        globals()[model_config](data, config, job_uuid)
        os.remove(config["data"])
        os.remove(config["data"][:-5] + ".csv") # remove -d and -df files
    except Exception as e:
        print(e)

def send_message(output_message, producer: KafkaProducer):
    """
    Send output message to output_topic
    """
    if os.environ.get("MICROML_DEBUG", "0"):
        print(f"format output message for sending to {output_topic}")
    producer.send(output_topic, output_message)

    
#######
def main():
    consumer = setup_kafka_consumer()
    producer = setup_kafka_producer()

    try:
        read_and_execute(consumer, producer)
    except Exception as e:
        print(e)
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    main()