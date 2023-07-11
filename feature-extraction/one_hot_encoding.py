import numpy as np
import pandas as pd
from sklearn.preprocessing import OneHotEncoder


def one_hot_encoding(data: pd.DataFrame, columns:list, new_columns: pd.DataFrame):
    """
    Uses OneHotEncoder from sklearn to convert given features to one hot encoded features in the specified columns.
    """

    encoder = OneHotEncoder(sparse_output=False).fit(new_columns[columns])

    try:
        new_data = pd.DataFrame(encoder.transform(new_columns[columns]))
        new_data.columns = encoder.get_feature_names_out()
        data = pd.concat([new_data, data], axis=1)
    except Exception as e:
        print("one_hot_encoding",e)
    return data