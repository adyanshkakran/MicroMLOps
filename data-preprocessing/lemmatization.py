import numpy as np
import pandas as pd
from nltk.stem import WordNetLemmatizer

lemmatizer = WordNetLemmatizer()

def lemmatization(data: pd.DataFrame, columns:list):
    """
    Uses WordNetLemmatizer from nltk to lemmatize words in the string from each column in columns list of the data df.
    Specified columns must consist of a single string for each row. Will fail if it is a list of words.
    Make sure to use lemmatization BEFORE tokenization.
    """
    try:
        for column in columns:
            data[data.columns[column]] = data[data.columns[column]].apply(lambda x: [lemmatizer.lemmatize(word) for word in x])
    except Exception as e:
        print(e)