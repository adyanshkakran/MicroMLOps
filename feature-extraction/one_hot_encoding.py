import numpy as np
import pandas as pd
from sklearn.preprocessing import OneHotEncoder


def one_hot_encoding(data: pd.DataFrame, columns:list, new_columns: pd.DataFrame, encoder: dict, logger, uuid: str="0") -> pd.DataFrame:
    """
    Uses OneHotEncoder from sklearn to convert given features to one hot encoded features in the specified columns.
    """

    logger.info("Performing one hot encoding", extra={"uuid": uuid})
    try:
        for column in columns:
            encoder[column] = OneHotEncoder(sparse_output=False).fit(new_columns[column])
            new_data = pd.DataFrame(encoder[column].transform(new_columns[column]))
            new_data.columns = encoder[column].get_feature_names_out()
            data = pd.concat([new_data, data], axis=1)
            new_columns.drop(column, axis=1, inplace=True)
        return data
    except Exception as e:
        logger.error(f"Error during one hot encoding: {str(e)}", extra={"uuid": uuid})
    return data