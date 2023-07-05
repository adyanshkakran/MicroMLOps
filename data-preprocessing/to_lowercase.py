import numpy as np
import pandas as pd

def to_lowercase(data: pd.DataFrame, columns:list):
    """
    Converts string in each column in the columns list of the data df
    to lower case.
    """
    try:
        for column in columns:
            data[data.columns[column]] = data[data.columns[column]].apply(lambda x: x.lower())
    except Exception as e:
        print(e)
    