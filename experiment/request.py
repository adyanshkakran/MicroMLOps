import requests
import pandas as pd
import time
import os
import yaml
import json

url = "http://localhost:8000/upload/"

def retrain():
    model_uuid = None
    with open('config-train.yaml', 'rb') as config:
        files = {'config_file': ("config.yaml", config)}
        
        r = requests.post(url, files=files)
        print('Retraining', r.status_code)

        res = json.loads(r.text)
        model_uuid = res['uuid']

        print('new model uuid', model_uuid)
    
    with open('config-inference.yaml', 'r') as config:
        config_data = yaml.safe_load(config)
        config_data['model'] = model_uuid
    
    with open('config-inference.yaml', 'w') as config:
        yaml.dump(config_data, config)
    
    time.sleep(15 * 60) # wait while training

def main():
    inference_data = pd.read_csv("../data-warehouse/data/test.csv")
    train_data = pd.read_csv("../data-warehouse/data/train-2.csv")
    train_data.to_csv("../data-warehouse/data/train-new.csv", index=False)
    del train_data

    new_train = open("../data-warehouse/data/train-new.csv", "a")

    retrain_time = 10 * 60 # send a retrain job every 10 minutes
    inference_time = 5 # send an inference every 5 seconds

    retrain()

    i = 0
    while i < 2000:
        str = inference_data.iloc[i][0].replace('"', '""')
        label = inference_data.iloc[i][1]
        
        with open("../data-warehouse/data/test-new.csv", "w") as new_infer:
            new_infer.write("text,label\n")
            new_infer.write(f'"{str}",{label}\n')

        with open('config-inference.yaml', 'rb') as config:
            files = {'config_file': ("config.yaml", config)}
            
            r = requests.post(url, files=files)
            print('Inferencing', r.status_code)
            if r.status_code == 500:
                i -= 1

        time.sleep(inference_time) 

        new_train.write(f'"{str}",{label}\n')

        if (i+1)*inference_time % retrain_time == 0:
            new_train.flush()
            retrain()

        i += 1

    new_train.close()
    os.remove("../data-warehouse/data/test-new.csv")
    os.remove("../data-warehouse/data/train-new.csv")

if __name__ == "__main__":
    main()