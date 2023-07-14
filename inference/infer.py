import numpy as np
import pandas as pd
import joblib
from pprint import pprint

def infer(data: pd.DataFrame, model_file_path: str, config: dict):
    """
    Loads model from model_file_path and predicts from data
    :return prediction results
    """

    try:
        model = joblib.load(model_file_path)
        results = model.predict(data)

        return pd.Series(results).to_json(orient="values")
    except FileNotFoundError as e:
        raise Exception("Could not find model") from e
    
