import pandas as pd
from itertools import groupby


def load_all_data(data_files, columns):
    """ Loads data from separete tsv files into one pandas dataframe.

    :param data_files: list of file names
    :type data_files: list(str)
    :param columns: list of columns SHOULD be in tsv files
    :type columns: list(str)
    :return: one pandas dataframe
    :rtype: pd.DataFrame
    """
    dataframe = pd.DataFrame(columns=columns)
    for data in data_files:
        read_file = pd.read_csv(
            data, sep="\t", header=None, index_col=None)
        read_file.columns = columns
        dataframe = pd.concat(
            [dataframe, read_file], axis=0)
    dataframe = dataframe.reset_index(drop=True)
    dataframe = dataframe.dropna(axis=0)
    return dataframe


def resolve_duplicates(dataframe):
    """ Resolves the duplicates issues

    :param dataframe: pandas dataframe with/out duplicates
    :type dataframe: pandas.DataFrame
    :return: cleaned dataframe
    :rtype: pandas.DataFrame
    """
    groups, _ = list(zip(*groupby(dataframe.user_name)))
    all_unique = len(set(groups)) == len(groups)
    print(f"All unique groups: {all_unique}")
    if not all_unique:
        duplicates = dataframe.duplicated(
            subset=None, keep='first')
        num_duplicates = duplicates.sum()
        print(f"Number of duplicates: {num_duplicates}")
        duplicate_counts = dataframe[duplicates].user_name.value_counts()
        print(f"By user name: \n{duplicate_counts}")
        dataframe = dataframe[~duplicates]
    dataframe = dataframe.reset_index(drop=True)
    return dataframe


def remove_different_chars(dataframe, column):
    """ Removes unwanted characters such as emojis, etc.

    :param dataframe: pandas dataframe
    :type dataframe: pandas.DataFrame
    :param column: name of the column
    :type column: str
    :return: cleaned dataframe
    :rtype: pandas.DataFrame
    """
    dataframe[column] = dataframe[column].apply(
        lambda x: x.encode('ascii', 'ignore').decode('ascii'))
    return dataframe

        # movie_dataframe['user_review'].apply(lambda x: x.encode('ascii', 'ignore').decode('ascii'))


def main():
    movie_data_files = ["data/movrec_mtan.tsv",
                        "data/movrec_emre.tsv"]

    columns = ["user_name", "movie_name", "release_year",
               "user_review", "user_rating", "review_date",
               "rewatched", "review_likes"]

    movie_dataframe_raw = load_all_data(movie_data_files, columns)
    movie_dataframe_uniques = resolve_duplicates(movie_dataframe_raw)
    movie_dataframe_cleaned = remove_different_chars(
        movie_dataframe_uniques, 'user_review')
    movie_dataframe_cleaned.to_csv("data/movie_data.tsv")


if __name__ == "main":
    main()
