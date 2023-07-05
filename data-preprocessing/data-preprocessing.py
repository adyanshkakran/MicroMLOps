"""
Template for microservices
Reads config from an input topic, 
performs operations and then
produces message on output topic
"""

# modules
import os
import json
import re
import numpy as np
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv

# functions to implement
from to_lowercase import to_lowercase
from remove_nonalphanumerics import remove_nonalphanumerics
from remove_stopwords import remove_stopwords
from tokenization import tokenization
from lemmatization import lemmatization
from stemming import stemming
from remove_specific import remove_specific


load_dotenv(override=True) # env file has higher preference

"""
configuring kafka
these must be set in env
default input topic is filename (without .py)
output topic must be specified
"""
input_topic:str = os.environ.get("INPUT_TOPIC", os.path.basename(__file__)[:-3])
output_topic:str = os.environ.get("OUTPUT_TOPIC", "default_output_topic")
kafka_broker:str = os.environ.get("KAFKA_BROKER", "localhost:9092")
consumer_group_id:str = os.environ.get("KCON_GROUP_ID", "default_group_id")

if os.environ.get("MICROML_DEBUG", "0"):
    print(f"Input Topic: {input_topic}; Output Topic: {output_topic}")
    print(f"Group ID: {consumer_group_id}; Kafka Broker: {kafka_broker}")

def setup_kafka_consumer():
    """
    Create and return a kafka consumer
    Uses global variables for config
    Uses specified input_topic, group_id, kafka_broker
    """
    config = {
        "group_id": consumer_group_id,
        "bootstrap_servers": kafka_broker
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
    obj = json.loads(message.value)
    data = pd.read_csv(obj["data"])
    path = obj["data"]

    execute(data, obj["data_preprocessing"], path)
    return obj

def execute(data: pd.DataFrame, config: dict, path: str):
    print("\n\n\n")
    for key in config.keys():
        if key == "remove_specific":
            try:
                regex = re.compile(config[key]["regex"])
                remove_specific(data, regex, config[key]["columns"])
            except Exception as e:
                print(f"Invalid regex {config[key]['regex']}")
        else:
            try: 
                globals()[key](data, config[key])
            except Exception as e:
                print(f"Function {key} not found")

    data.to_csv(path[:-4] + "-d.csv", index=False) # saves in file originalFileName-d.csv

    if os.environ.get("MICROML_DEBUG"):
        print(data.head())

def send_message(output_message, producer: KafkaProducer):
    """
    Send output message to output_topic
    """
    if os.environ.get("MICROML_DEBUG", "0"):
        print(f"format {output_message} for sending")
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