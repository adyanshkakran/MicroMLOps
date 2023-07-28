import pandas as pd
import time
import os
import json
from sklearn.svm import SVC
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import joblib

def svm(data: pd.DataFrame, config: dict, logger, job_uuid: str):
    debug_mode:bool = os.environ.get("MICROML_DEBUG", "0") == "1"
    
    training_config = config.get("training")
    labels = data[training_config.get("labels")]
    features = data[data.columns.difference(training_config.get("labels"))]
    x_train, x_test, y_train, y_test = train_test_split(features, labels, test_size=0.2, random_state=42)
    if debug_mode:
        logger.debug(f"shapes: x_train:{x_train.shape}, x_test:{x_test.shape}, y_train:{y_train.shape}, y_test:{y_test.shape}", extra={"uuid": job_uuid})

    # Train svc and measure time to train
    model = SVC()
    start_time = time.time()
    model.fit(x_train, y_train)
    stop_time = time.time()

    if debug_mode:
        logger.debug(f"Took {stop_time - start_time:.2f}s to train", extra={"uuid": job_uuid})
    
    # test and record accuracy
    predictions = model.predict(x_test)
    accuracy = accuracy_score(y_test, predictions)
    if debug_mode:
        logger.debug(f"Accuracy: {accuracy * 100:.2f}", extra={"uuid": job_uuid})

    # write model and config to files
    model_name = os.environ.get("MODEL_WAREHOUSE") + job_uuid + ".model"
    info_name = os.environ.get("INFO_WAREHOUSE") + job_uuid + ".info"
    config["accuracy"] = accuracy

    joblib.dump(model, model_name)

    with open(info_name, "w+") as f:
        json.dump(config, f, ensure_ascii=False)

    if debug_mode:
        logger.debug("Saved model and info files", extra={"uuid": job_uuid})