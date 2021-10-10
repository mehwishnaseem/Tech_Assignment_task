import json
import pymysql


conn = pymysql.connect(
    host="collinson-test-db.cndinttvmdbp.us-east-2.rds.amazonaws.com",
    user="admin",
    password="12345678", 
    database="json_db",
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor) # return rows in dict format instead of tuple


def build_query(params):
  """
  Non sanitized query building function
  takes dict and return query string
  """
  columns = ', '.join(params['col'])
  query = f'SELECT {columns} FROM {params.get("table", "tbl_tweet_df")}'
  if params.get('where', None):
    query = query + f' WHERE {params["where"]}'
  if params.get('groupby', None):
    query = query + f' GROUPBY {params["groupby"]}'
  if params.get('orderby', None):
    order_cols = ', '.join([k+' '+v for k, v in eval(params['orderby']).items()])
    query = query + f' ORDER BY {order_cols}'
  query = query + ' LIMIT 0, 1000;'
  return query


def lambda_handler(event, context):
    """AWS Lambda function
    Example:
    {"col": ["text","retweet_count"],
    "table": "tbl_tweet_df"}
    """
    query = build_query(event)
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    return rows

    