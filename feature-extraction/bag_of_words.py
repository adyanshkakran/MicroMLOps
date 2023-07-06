import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer


def bag_of_words(data: pd.DataFrame, columns:list, new_columns: pd.DataFrame):
    """
    Uses CountVectorizer from sklearn to convert given features to tf-idf features in the specified columns.
    """

    vectorizer = CountVectorizer(analyzer='word', stop_words='english')

    try:
        for column in columns:
            tf_column = vectorizer.fit_transform(new_columns.iloc[:, column])
            tf_df = pd.DataFrame(tf_column.toarray(), columns=vectorizer.get_feature_names_out())

            data = pd.concat([tf_df, data], axis=1)
    except Exception as e:
        print("bag_of_words",e)
    return data