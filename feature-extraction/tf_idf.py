import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer


def TF_IDF(data: pd.DataFrame, columns:list, new_columns: pd.DataFrame):
    """
    Uses TfidfVectorizer from sklearn to convert given features to tf-idf features in the specified columns.
    """

    vectorizer = TfidfVectorizer(analyzer='word', stop_words='english')
    logger.info("Performing tfidf", extra={"uuid": uuid})
    try:
        for column in columns:
            tf_column = vectorizer.fit_transform(new_columns[column])
            tf_df = pd.DataFrame(tf_column.toarray(), columns=vectorizer.get_feature_names_out())

            data = pd.concat([tf_df, data], axis=1)
    except Exception as e:
        # print("TF_IDF",e)
        logger.error(f"Error during tfidf: {str(e)}", extra={"uuid": uuid})
    return data