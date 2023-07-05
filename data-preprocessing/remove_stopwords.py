import numpy as np
import pandas as pd
from nltk.corpus import stopwords
eng_stopwords = stopwords.words("english")

def remove_stopwords(data: pd.DataFrame, columns: list):
    """
    Removes words present in english stopwords list of nltk.corpus
    from each column in the columns list of the data df.
    Each row in these columns must consist of a single string.
    Use remove_stopwords BEFORE tokenization
    """
    try:
        for column in columns:
            data[data.columns[column]] = data[data.columns[column]].apply(lambda x: " ".join([word for word in x.split() if word not in eng_stopwords]))
    except Exception as e:
        print(e)
    