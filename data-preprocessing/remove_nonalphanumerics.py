import numpy as np
import pandas as pd
import re

def remove_nonalphanumerics(data:pd.DataFrame, columns:list, logger, uuid:str="0"):
    """
    Uses regex to remove non alphanumeric characters (not a-z, A-Z or 0-9) from
    each column in the list of columns in the data df
    """
    try:
        logger.debug("Removing non alphanumeric characters", extra={"uuid": uuid})
        for column in columns:
            data[data.columns[column]] = data[data.columns[column]].apply(lambda x: re.sub("[^a-zA-Z0-9]"," ",x))
    except Exception as e:
        # print(e)
        logger.error(str(e), extra={"uuid": uuid})
