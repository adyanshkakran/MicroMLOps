import numpy as np
import pandas as pd
import re

def remove_specific(data: pd.DataFrame, regex: re.Pattern, columns: list):
    """
    Applies given regex to remove characters from each column from given columns list
    of data df.
    Each row in the columns must consist of a single string.
    Use remove_specific BEFORE tokenization.
    """
    try:
        for column in columns:
            data[data.columns[column]] = data[data.columns[column]].apply(lambda x: re.sub(regex , " ", x))
    except Exception as e:
        print(e)