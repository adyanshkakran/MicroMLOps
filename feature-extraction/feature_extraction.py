"""
Template for microservices
Reads config from an input topic, 
performs operations and then
produces message on output topic
"""

import os
import json
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
import pandas as pd

from tf_idf import TF_IDF
from one_hot_encoding import one_hot_encoding
from bag_of_words import bag_of_words
import joblib

load_dotenv(override=True) # env file has higher preference

"""
configuring kafka
these must be set in env
default input topic is filename (without .py)
output topic must be specified
"""
input_topic:str = os.environ.get("INPUT_TOPIC", os.path.basename(__file__)[:-3])
output_topic_inference:str = os.environ.get("INFERENCE_OUTPUT_TOPIC", "default_output_topic")
output_topic_training:str = os.environ.get("TRAINING_OUTPUT_TOPIC", "default_output_topic")
kafka_broker:str = os.environ.get("KAFKA_BROKER", "localhost:9092")
consumer_group_id:str = os.environ.get("KCON_GROUP_ID", "default_group_id")

if os.environ.get("MICROML_DEBUG", "0"):
    print(f"Input Topic: {input_topic}; Output Topic(t/i): {output_topic_training}/{output_topic_inference}")
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
            output_message, output_topic = process_message(message)
            send_message(output_message, output_topic, producer)
    except KeyboardInterrupt:
        print("Exiting...")

def process_message(message):
    """
    Read incoming message
    Do necessary computation
    Return output message
    """
    message_obj = json.loads(message.value)
    data = pd.read_csv(message_obj["data"])
    path = message_obj["data"]
    
    if message_obj["action"] == "inference":
        execute_inference(data, message_obj.get("feature_extraction"), path, message_obj["model"])
    elif message_obj["action"] == "training":
        execute_training(data, message_obj.get("feature_extraction"), path, message_obj["uuid"])

    message_obj["data"] = path[:-4] + "f.csv" # saves in file originalFileName-df.csv

    if message_obj["action"] == "training":
        return message_obj, output_topic_training
    elif message_obj["action"] == "inference":
        return message_obj, output_topic_inference

def execute_training(data: pd.DataFrame, config: dict, path: str, uuid: str):
    print("\n\n\n")
    done_columns = []
    columns = set()
    for key in config.keys():
        if key == "drop":
            continue
        columns.update(config[key])
    new_columns = pd.DataFrame(data[list(columns)])
    data.drop(list(columns), axis=1, inplace=True)
    encoders = {}
    for key in config.keys():
        for col in config[key]:
            # if key == "bag_of_n_grams":
            #     col = col[0]
            if col in done_columns:
                print(f"Skipping {col} for {key} as it has already been processed.")
                config[key].remove(col)
                continue
            if key == "drop":
                data.drop(col, axis=1, inplace=True)
                print(f"Dropped {col}")
        done_columns.extend(config[key])
        if key != "drop":
            data = globals()[key](data, config[key], new_columns, encoders)

    try:
        data.to_csv(path[:-4]+ "f.csv", index=False)
    except Exception as e:
        print("feature_extraction",e)

    transformer_path = os.environ.get("ENCODER_WAREHOUSE", ".") + uuid + ".encoder"
    joblib.dump(encoders, transformer_path)

    if os.environ.get("MICROML_DEBUG", "0"):
        print(data.head())
        print("Transformer saved at: ", transformer_path)

def execute_inference(data: pd.DataFrame, config: dict, path: str, uuid: str):
    encoder = joblib.load(os.environ.get("ENCODER_WAREHOUSE", ".") + uuid + ".encoder")
    # print(encoder)
    for item in encoder.keys():
        new_matrix = encoder[item].transform(data[item.split(":")[1]])
        new_df = pd.DataFrame(new_matrix.toarray(), columns=encoder[item].get_feature_names_out())
        data.drop(item.split(":")[1], axis=1, inplace=True)
        data = pd.concat([new_df, data], axis=1)
    try:
        data.to_csv(path[:-4]+ "f.csv", index=False)
    except Exception as e:
        print("feature_extraction",e)
        exit(0)

    if os.environ.get("MICROML_DEBUG", "0"):
        print(data.head())

def send_message(output_message, output_topic, producer: KafkaProducer):
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