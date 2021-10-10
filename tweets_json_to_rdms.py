"""
This scripts parses twitter tweets from JSON format and store data in AWS RDS(MYSQL).
9 tables are created to store data. Nested fields such as user, entities etc converted to seperate table
containing tweetid/userid to preserve relation between nested item)
"""
import os
import gzip
import json
import fnmatch
from re import S
from numpy.core.arrayprint import SubArrayFormat
import pandas as pd

from tqdm import tqdm
from sqlalchemy import create_engine


NESTED_COLS = ['user', 'entities', 'extended_entities', 'extended_tweet', 'coordinates', 'place',
    'quoted_status', 'retweeted_status']


def conv_list_to_str(df):
    """Converts columns with list dtype to string for storing in database
    """
    for col in df.columns:
        if df[col].dtype == list or df[col].dtype == dict:
            df[col] = df[col].astype('str')
    return df


def process_nested_col(nested_col, main_df):
    """Converts nested json fields into separate columns

    Args:
        nested_col (str): column name to be processed
        main_df (DataFrame): dataframe constructed from json file

    Returns:
        DataFrame: dataframe constructed from a single column with nested values
    """
    df = pd.json_normalize(
        main_df[nested_col]
        .apply(lambda x: {} if pd.isnull(x) else x)
        )
    # copy user id to main table
    if nested_col == 'user':
        main_df['my_user_id'] = pd.Series(df['id_str'])
    # copy the tweet id to each table of nested table
    df['my_tweet_id'] = pd.Series(main_df['id_str'])
    df = df.dropna(subset=[col for col in df.columns if col != 'my_tweet_id'], how='all')
    df = conv_list_to_str(df)
    return df


def load_json_data(json_file):
    """Converts tweets json file to list of python dicts and
    then constructs ands return a pandas datafram from it

    Args:
        json_file (str): json file path
        
    Returns:
        DataFrame
    """
    data =[]
    with gzip.open(json_file) as f:
        for line in f.readlines():
            data.append(json.loads(line))
    tmp_df = pd.DataFrame(data)
    return tmp_df
    
def get_dtypes_in_df(df):
    """Utility function to print different dtypes in a column with type object
    """
    for col in df.columns:
        if isinstance(df[col].dtype, object):
            dts = set(df[col].apply(lambda x: type(x).__name__))
            if 'dict' in dts or 'list' in dts:
                print(col, dts)


def main():
    script_path = os.path.abspath(os.path.dirname(__file__))
    json_folder = os.path.join(script_path, 'json_folder')

    cnx = create_engine(
        'mysql+pymysql://admin:12345678@collinson-test-db.cndinttvmdbp.us-east-2.rds.amazonaws.com:3306/json_db', 
        echo=False
        )
    
    json_files = []
    for root, dirnames, filenames in os.walk(json_folder):
        for filename in fnmatch.filter(filenames, '*gz'):
            json_files.append(os.path.join(root, filename))
    
    # json_files = json_files[:10] # parsing 10 files for testing
    
    json_df = pd.DataFrame()
    for json_file in tqdm(json_files,  total=len(json_files)):
        tmp_df = load_json_data(json_file)
        json_df = json_df.append(tmp_df, ignore_index=True, sort=True)
    
    # id_str should be unique (https://developer.twitter.com/en/docs/twitter-api/v1/data-dictionary/object-model/tweet)
    json_df = json_df.drop_duplicates(subset=['id_str'])
    
    print('processing nested columns')
    user_df = process_nested_col('user', json_df)
    entities_df = process_nested_col('entities', json_df)
    ext_ent_df = process_nested_col('extended_entities', json_df)
    ext_tweet_df = process_nested_col('extended_tweet', json_df)
    quoted_df = process_nested_col('quoted_status', json_df)
    retweet_df = process_nested_col('retweeted_status', json_df)
    coord_df = process_nested_col('coordinates', json_df)
    place_df = process_nested_col('place', json_df)
    
    # remove geo as it is now redundant column
    tweet_df = json_df.drop(columns=NESTED_COLS+['geo'])
    tweet_df = conv_list_to_str(tweet_df)

    print('writing to database')
    tweet_df.to_sql(name='tbl_tweet_df', con=cnx, if_exists='replace', index=False)
    user_df.to_sql(name='tbl_user_df', con=cnx, if_exists='replace', index=False)
    entities_df.to_sql(name='tbl_entities_df', con=cnx, if_exists='replace', index=False)
    ext_ent_df.to_sql(name='tbl_ext_ent_df', con=cnx, if_exists='replace', index=False)
    ext_tweet_df.to_sql(name='tbl_ext_tweet_df', con=cnx, if_exists='replace', index=False)
    quoted_df.to_sql(name='tbl_quoted_df', con=cnx, if_exists='replace', index=False)
    retweet_df.to_sql(name='tbl_retweet_df', con=cnx, if_exists='replace', index=False)
    coord_df.to_sql(name='tbl_coord_df', con=cnx, if_exists='replace', index=False)
    place_df.to_sql(name='tbl_place_df', con=cnx, if_exists='replace', index=False)


if __name__ == "__main__":
    main()