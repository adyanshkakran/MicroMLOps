import numpy as np
import pandas as pd
import joblib
from pprint import pprint
import json
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score

def infer(data: pd.DataFrame, model_file_path: str, logger, uuid: str="0", labels = None):
    """
    Loads model from model_file_path and predicts from data
    :return prediction results
    """

    logger.info("Conducting inference", extra={"uuid": uuid})
    results = {}
    try:
        model = joblib.load(model_file_path)
        pred_prob = model.predict_proba(data)

        predictions = np.argmax(pred_prob, axis=1).tolist()

        results["predictions"] = predictions
        results["confidence"] = pred_prob.tolist()

        if labels is not None:
            results["accuracy"] = float(accuracy_score(labels, predictions))
            results["f1_score"] = float(f1_score(labels, predictions))
            results['precision'] = float(precision_score(labels,predictions))
            results['recall'] = float(recall_score(labels,predictions))

        return results
    except FileNotFoundError as e:
        logger.error("Could not find model", extra={"uuid": uuid})
        raise Exception("Could not find model") from e
    except AttributeError as e:
        logger.error("Model does not have predict confidence method", extra={"uuid": uuid})
        results = model.predict(data)

        return pd.Series(results).to_json(orient="values")
    except Exception as e:
        logger.error(f"Unknown error {e}", extra={"uuid": uuid})
        raise Exception("Unknown error") from e
