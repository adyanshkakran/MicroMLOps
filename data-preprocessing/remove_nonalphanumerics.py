import numpy as np
import pandas as pd
import re

def remove_nonalphanumerics(data: pd.DataFrame, columns: list):
    """
    Uses regex to remove non alphanumeric characters (not a-z, A-Z or 0-9) from
    each column in the list of columns in the data df
    """
    try:
        for column in columns:
            data[data.columns[column]] = data[data.columns[column]].apply(lambda x: re.sub("[^a-zA-Z0-9]"," ",x))
    except Exception as e:
        print(e)