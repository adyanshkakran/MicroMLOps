import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer


def TF_IDF(data: pd.DataFrame, columns:list, new_columns: pd.DataFrame, encoders:dict) -> pd.DataFrame:
    """
    Uses TfidfVectorizer from sklearn to convert given features to tf-idf features in the specified columns.
    """

    try:
        for column in columns:
            vectorizer = TfidfVectorizer(analyzer='word', stop_words='english')

            tf_column = vectorizer.fit_transform(new_columns[column])
            tf_df = pd.DataFrame(tf_column.toarray(), columns=vectorizer.get_feature_names_out())

            new_columns.drop(column, axis=1, inplace=True)
            data = pd.concat([tf_df, data], axis=1)
            
            encoders[f"TF_IDF:{column}"] = vectorizer
        return data
    except Exception as e:
        print("TF_IDF",e)