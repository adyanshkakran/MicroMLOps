import yaml
from kafka import KafkaProducer
import json

producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),bootstrap_servers=['localhost:9092'])

def checkSteps(steps, standard, step):
    for key in steps:
        if key not in standard:
            raise Exception('Invalid ' + step + ' step ' + key)

def checkYaml():
    with open('config.yaml') as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
        with open('specification.yaml') as st:
            standard = yaml.load(st, Loader=yaml.FullLoader)
            if data['action'] == 'training':
                checkSteps(data['data-preprocessing'], standard['data-preprocessing'], 'data-preprocessing')
                checkSteps(data['feature-extraction'], standard['feature-extraction'], 'feature-extraction')
                checkSteps(data['training'], standard['training'], 'training')
                if data['data-preprocessing'] is not None:     
                    producer.send('data-preprocessing', data)
                    return
                if data['feature-extraction'] is not None:
                    producer.send('feature-extraction', data)
                    return
                if data['training'] is not None:
                    producer.send('training', data)
                    return
            elif data['action'] == 'inference':
                for i in ['data-preprocessing', 'feature-extraction', 'training']:
                    if data[i] is not None:
                        raise Exception('Invalid ' + i + ' step')
            else:
                raise Exception('Invalid action')
checkYaml()
producer.flush()
    