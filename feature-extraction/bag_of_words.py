import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer


def bag_of_words(data: pd.DataFrame, columns:list, new_columns: pd.DataFrame, encoders:dict, logger, uuid: str = "0") -> pd.DataFrame:
    """
    Uses CountVectorizer from sklearn to convert given features to tf-idf features in the specified columns.
    """

    logger.info("Performing bag of words", extra={"uuid": uuid})
    try:
        for column in columns:
            vectorizer = CountVectorizer(analyzer='word', stop_words='english')

            tf_column = vectorizer.fit_transform(new_columns[column])
            tf_df = pd.DataFrame(tf_column.toarray(), columns=vectorizer.get_feature_names_out())

            new_columns.drop(columns, axis=1, inplace=True)
            data = pd.concat([tf_df, data], axis=1)

            encoders[f"bag_of_words:{column}"] = vectorizer
        return data
    except Exception as e:
        # print("bag_of_words",e)
        logger.error(f"Error during bag of words: {str(e)}", extra={"uuid": uuid})
    return data
