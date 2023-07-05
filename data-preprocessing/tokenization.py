import numpy as np
import pandas as pd

def tokenization(data: pd.DataFrame, columns: list):
    """
    Word tokenization using the .split() method.
    Apply lemmatization/stemming/any regex BEFORE tokenizing.
    """
    try:
        for column in columns:
            # pass
            data[data.columns[column]] = data[data.columns[column]].apply(lambda x: [word for word in x.split()])
    except Exception as e:
        print(e)