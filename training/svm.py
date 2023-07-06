import pandas as pd
import time
import os
import json
from sklearn.svm import SVC
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import joblib

def svm(data: pd.DataFrame, config: dict, job_uuid: str):
    features = data[data.columns[:-1]]
    x_train, x_test, y_train, y_test = train_test_split(features, data[data.columns[-1]])
    if os.environ.get("MICROML_DEBUG", "0"):
        print("shapes: x_train, x_test, y_train, y_test", x_train.shape, x_test.shape, y_train.shape, y_test.shape)

    # Train svc and measure time to train
    model = SVC()
    start_time = time.time()
    model.fit(x_train, y_train)
    stop_time = time.time()

    if os.environ.get("MICROML_DEBUG", "0"):
        print(f"Took {stop_time - start_time:.2f}s to train")
    
    # test and record accuracy
    predictions = model.predict(x_test)
    accuracy = accuracy_score(y_test, predictions)
    if os.environ.get("MICROML_DEBUG", "0"):
        print(f"Accuracy: {accuracy * 100:.2f}")

    # write model and config to files
    model_name = os.environ.get("MODEL_WAREHOUSE") + job_uuid + ".model"
    info_name = os.environ.get("INFO_WAREHOUSE") + job_uuid + ".info"
    config["accuracy"] = accuracy

    joblib.dump(model, model_name)

    with open(info_name, "w+") as f:
        json.dump(config, f, ensure_ascii=False)

    if os.environ.get("MICROML_DEBUG", "0"):
        print("Saved model and info files")