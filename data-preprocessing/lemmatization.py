import numpy as np
import pandas as pd
from nltk.stem import WordNetLemmatizer

lemmatizer = WordNetLemmatizer()

def lemmatization(data:pd.DataFrame, columns:list, logger, uuid:str="0"):
    """
    Uses WordNetLemmatizer from nltk to lemmatize words in the string from each column in columns list of the data df.
    Specified columns must consist of a single string for each row. Will fail if it is a list of words.
    Make sure to use lemmatization BEFORE tokenization.
    """
    try:
        logger.debug("Performing lemmatization", extra={"uuid": uuid})
        for column in columns:
            data[data.columns[column]] = data[data.columns[column]].apply(lambda x: " ".join([lemmatizer.lemmatize(word) for word in x.split()]))
    except Exception as e:
        # print(e)
        logger.error(str(e), extra={"uuid": uuid})
