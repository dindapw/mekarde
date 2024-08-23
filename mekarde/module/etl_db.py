import logging
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from mekarde.module.etl_common import get_parameter, get_redshift_connection, get_s3_client, s3_to_redshift

logging.basicConfig(level=logging.INFO)
filepath = os.path.dirname(os.path.abspath(__file__ + '/../')) + '/file'


def get_df_from_query(query):
    conn = get_redshift_connection()
    with conn.cursor() as cursor:
        cursor.execute(query)
        df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

    conn.close()
    return df


def df_to_s3(df, target_schema, target_table):
    temp_csv_filepath = f'/tmp/{target_schema}_{target_table}_{time.time()}.csv'
    df.to_csv(temp_csv_filepath, index=False)

    s3_client = get_s3_client()
    s3_bucket = get_parameter('/s3/data/bucket')
    s3_key = f'{target_schema}/{target_table}_{datetime.now().strftime("%Y%m%d")}.csv'
    s3_uri = f's3://{s3_bucket}/{s3_key}'

    s3_client.upload_file(temp_csv_filepath, s3_bucket, s3_key)
    return s3_uri


def replicate(load_data_query, target_schema, target_table, create_table_query):
    query = Path(f'{filepath}/{load_data_query}').read_text()
    df = get_df_from_query(query)
    header = df.columns.tolist()
    s3_uri = df_to_s3(df, target_schema, target_table)
    create_table_query = Path(f'{filepath}/{create_table_query}').read_text()
    columns = ', '.join([f'{col_name}' for col_name in header])
    columns = f'({columns})'
    s3_to_redshift(s3_uri, target_schema, target_table, create_table_query, columns)


# if __name__ == '__main__':
#     replicate('dwh_transactions_data.sql',
#               'datawarehouse',
#               'transactions',
#               'dwh_transactions_table.sql')
