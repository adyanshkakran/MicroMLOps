import numpy as np
import pandas as pd
from nltk.stem import PorterStemmer

stemmer = PorterStemmer()

def stemming(data: pd.DataFrame, columns: list):
    """
    Uses PorterStemmer from nltk to stem words in the string from each column in columns list of the data df.
    Specified columns must consist of a single string for each row. Will fail if it is a list of words.
    Make sure to use stemming BEFORE tokenization.
    """
    try:
        for column in columns:
            data[data.columns[column]] = data[data.columns[column]].apply(lambda x: " ".join([stemmer.stem(word) for word in x.split()]))
    except Exception as e:
        print(e)