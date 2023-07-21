import numpy as np
import pandas as pd

def tokenization(data: pd.DataFrame, columns: list, logger, uuid:str="0"):
    """
    Word tokenization using the .split() method.
    Apply lemmatization/stemming/any regex BEFORE tokenizing.
    """
    try:
        logger.debug("Performing tokenization", extra={"uuid": uuid})
        for column in columns:
            data[data.columns[column]] = data[data.columns[column]].apply(lambda x: [word for word in x.split()])
    except Exception as e:
        # print(e)
        logger.error(str(e), extra={"uuid": uuid})
