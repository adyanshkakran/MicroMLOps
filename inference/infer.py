import numpy as np
import pandas as pd
import joblib

def infer(data: pd.DataFrame, model_file_path: str, logger, uuid: str="0"):
    """
    Loads model from model_file_path and predicts from data
    :return prediction results
    """
    # from sklearn.feature_extraction.text import CountVectorizer
    # vectorizer = CountVectorizer(max_features=10000)
    # BOW = vectorizer.fit_transform(data[data.columns[0]])

    logger.info("Conducting inference", extra={"uuid": uuid})
    try:
        model = joblib.load(model_file_path)
        results = model.predict(data[data.columns[:-1]])

        return pd.Series(results).to_json(orient="values")
    except FileNotFoundError as e:
        logger.error("Could not find model", extra={"uuid": uuid})
        raise Exception("Could not find model") from e
