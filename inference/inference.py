import json
import os
import time
import logging
import numpy as np
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
import pyRAPL
import csv

from kafka_logger import configure_logger

from infer import infer

load_dotenv(override=True) # env file has higher preference
pyRAPL.setup()

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

if debug_mode:
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
        logger.critical("Failed to create Kafka Consumer")
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
        logger.critical("Failed to create Kafka Producer")
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
        # print("Exiting...")
        logger.info("Received KeyboardInterrupt. Exiting.")

def process_message(message):
    """
    Read incoming message
    Do necessary computation
    Return output message
    """
    message_obj = json.loads(message.value)
    
    job_uuid = message_obj["model"]
    data = pd.read_csv(message_obj["data"])

    try:
        model_file_path = os.environ.get("MODEL_WAREHOUSE") + job_uuid + ".model"
        info_file_path = os.environ.get("INFO_WAREHOUSE") + job_uuid + ".info"

        if debug_mode:
            # print(model_file_path, info_file_path)
            logger.debug(f"Model: {model_file_path} , info: {info_file_path}", extra={"uuid": job_uuid})
    except Exception as e:
        logger.error("Could not find model or info", {"uuid": job_uuid})
        raise Exception("Could not find model or info") from e
    
    labels = None
    if message_obj.get('labels') is not None:
        labels = data[message_obj.get('labels')]
        data = data.drop(columns=message_obj.get('labels'))

    measure = pyRAPL.Measurement('cpu')

    measure.begin()
    results = infer(data, model_file_path, logger, message_obj,labels=labels)
    measure.end()

    results["power"] = {
        "cpu (µJ)": measure.result.pkg[0],
        "dram": measure.result.dram[0],
        "time": measure.result.duration
    }

    os.remove(message_obj["data"])
    os.remove(message_obj["data"][:-5] + ".csv") # remove -d and -df files

    with open('../archive/results/results.csv', 'a') as f:
        writer = csv.writer(f)
        output = [time.time(),'inference', results['confidence'], results['accuracy'],results['f1_score'],results['precision'], results['recall'], results['power']['cpu (µJ)'], results['power']['dram'], results['power']['time']]
        writer.writerow(output)

    return json.dumps(results, ensure_ascii=False)

def send_message(output_message, producer: KafkaProducer):
    """
    Send output message to output_topic and save results
    """
    if debug_mode:
        # print(f"format {output_message} for sending")
        logger.debug(f"Sending {output_message}")
    producer.send(output_topic, output_message)

    
#######
def main():
    consumer = setup_kafka_consumer()
    producer = setup_kafka_producer()

    try:
        read_and_execute(consumer, producer)
    except Exception as e:
        # print(e)
        logger.error(str(e))
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    main()
