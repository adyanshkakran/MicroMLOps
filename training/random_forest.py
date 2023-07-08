import pandas as pd
import time
import os
import json
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import joblib

def random_forest(data: pd.DataFrame, config: dict, job_uuid: str):
    training_config = config.get("training")
    labels = data[training_config.get("labels")]
    features = data[data.columns.difference(training_config.get("labels"))]
    x_train, x_test, y_train, y_test = train_test_split(features, labels, test_size=0.2, random_state=42)
    if os.environ.get("MICROML_DEBUG", "0"):
        print("shapes: x_train, x_test, y_train, y_test", x_train.shape, x_test.shape, y_train.shape, y_test.shape)

    n_estimators = training_config.get("n_estimators")
    max_depth = training_config.get("max_depth")
    if n_estimators is None:
        n_estimators = 100
    if max_depth == 'None':
        max_depth = None
    model = RandomForestClassifier(n_estimators=n_estimators, max_depth=max_depth)
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