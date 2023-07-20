import yaml
import json
import uuid
import os
import time
import logging
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka_logger import configure_logger

load_dotenv(override=True) # env file has higher preference
logs_topic:str = os.environ.get("LOGS_TOPIC", "logs")
kafka_broker:str = os.environ.get("KAFKA_BROKER", "localhost:9092")
debug_mode:bool = os.environ.get("MICROML_DEBUG", "0") == "1"

time.sleep(25)
logger = configure_logger("yaml_reader", logs_topic, [kafka_broker], level=logging.DEBUG if debug_mode else logging.INFO)
logger.info("done waiting for kafka")

producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf_8'),bootstrap_servers=[os.environ.get("KAFKA_BROKER", "localhost:9092")])

def checkSteps(steps, standard, step):
    if steps is None:
        return
    for key in steps:
        if key not in standard:
            raise Exception('Invalid ' + step + ' step ' + key)

def checkYaml():
    with open('config.yaml') as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
        with open('specification.yaml') as st:
            standard = yaml.load(st, Loader=yaml.FullLoader)
            if data['action'] == 'training':
                checkSteps(data.get('data_preprocessing'), standard['data_preprocessing'], 'data_preprocessing')
                checkSteps(data.get('feature_extraction'), standard['feature_extraction'], 'feature_extraction')
                checkSteps(data.get('training'), standard['training'], 'training')
                data["uuid"] = str(uuid.uuid4())
                if data.get('data_preprocessing') is not None:     
                    producer.send('data_preprocessing', data)
                    return
                if data.get('feature_extraction') is not None:
                    producer.send('feature_extraction', data)
                    return
                if data.get('training') is not None:
                    producer.send('training', data)
                    return
            elif data['action'] == 'inference':
                for i in ['data_preprocessing', 'feature_extraction', 'training']:
                    if data[i] is None:
                        raise Exception('Invalid ' + i + ' step')
                if data.get('data_preprocessing') is not None:     
                    producer.send('data_preprocessing', data)
                    return
                if data.get('feature_extraction') is not None:
                    producer.send('feature_extraction', data)
                    return
            else:
                raise Exception('Invalid action')
checkYaml()
logger.info("done reading config")
producer.flush()
