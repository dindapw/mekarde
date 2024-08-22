import logging
import os
import time

import boto3
import pandas as pd
import redshift_connector
from pathlib import Path
from string import Template

from mekarde.module.etl_common import get_parameter, get_redshift_connection

logging.basicConfig(level=logging.INFO)
filepath = os.path.dirname(os.path.abspath(__file__ + '/../')) + '/file'


def construct_source_s3_detail(s3_uri):
    bucket_name, s3_key = s3_uri[len("s3://"):].split("/", 1)

    s3_folder = os.path.dirname(s3_key)
    s3_filename = os.path.splitext(os.path.basename(s3_key))
    s3_file_extension = s3_filename[1]

    source = {
        "s3_uri": s3_uri,
        "s3_bucket": bucket_name,
        "s3_key": s3_key,
        "s3_folder": s3_folder,
        "s3_filename": s3_filename[0],
        "s3_file_extension": s3_file_extension
    }
    logging.info(f'construct_source_s3_detail, result={source}')
    return source


def construct_target_s3_detail(target_schema, target_table):
    s3_bucket = get_parameter('/s3/data/bucket')
    s3_folder = target_schema
    s3_filename = target_table
    s3_file_extension = '.csv'
    s3_key = f"{s3_folder}/{s3_filename}{s3_file_extension}"
    target = {
        "s3_uri": f"s3://{s3_bucket}/{s3_key}",
        "s3_bucket": s3_bucket,
        "s3_key": s3_key,
        "s3_folder": s3_folder,
        "s3_filename": s3_filename,
        "s3_file_extension": '.csv'
    }
    logging.info(f'construct_target_s3_detail, result={target}')
    return target


def transform_column_name(columns):
    new_columns = []
    for each in columns:
        new_each = each.lower().replace(' ', '_')
        new_each = new_each.replace('-', '_')
        new_each = new_each.replace('.', '_')
        new_columns.append(new_each)
    return new_columns


def s3_to_redshift(s3_uri, schema_name, table_name):
    logging.info(f's3_to_redshift, event=start, s3_uri={s3_uri}, schema_name={schema_name}, table_name={table_name}')

    logging.info(f's3_to_redshift, message="start constructing list of columns"')
    df = pd.read_csv(s3_uri, nrows=0)
    header = df.columns.tolist()
    columns = ', '.join([f'{col_name}' for col_name in transform_column_name(header)])
    logging.info(f's3_to_redshift, message="finish constructing list of columns", columns={columns}')

    conn = get_redshift_connection()
    conn.autocommit = True
    cursor = conn.cursor()

    truncate_query = Template(Path(f'{filepath}/truncate_table.sql').read_text()).substitute(
        table_name=f'{schema_name}.{table_name}'
    )

    access_key = get_parameter('/s3/access_key')
    secret_key = get_parameter('/s3/secret_key')
    copy_query = Template(Path(f'{filepath}/copy_s3_to_table.sql').read_text()).substitute(
        table_name=f'{schema_name}.{table_name}',
        columns=columns,
        s3_uri=s3_uri,
        access_key=access_key,
        secret_key=secret_key
    )
    copy_query_log = copy_query.replace(access_key, 'xxxxx').replace(secret_key, 'xxxxx')

    try:
        logging.info(f's3_to_redshift, message="start truncating table", query={truncate_query}')
        cursor.execute(truncate_query)
        logging.info(f's3_to_redshift, message="finish truncating table"')

        logging.info(f's3_to_redshift, message="start copy query", query={copy_query_log}')
        cursor.execute(copy_query)
        logging.info(f's3_to_redshift, message="finish copy query"')

    except redshift_connector.error.ProgrammingError as e:
        logging.error(f's3_to_redshift, error={e}')
        error_message = e.args[0].get('M', '')
        if error_message == 'Cannot COPY into nonexistent table test':
            columns_create_table = ', '.join([f'{col_name} VARCHAR(max)' for col_name in transform_column_name(header)])
            columns_create_table += ', load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
            create_table_sql = f'CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({columns_create_table});'
            logging.info(f's3_to_redshift, message="start create table", query={create_table_sql}')
            cursor.execute(create_table_sql)
            logging.info(f's3_to_redshift, message="finish create table"')
            logging.info(f's3_to_redshift, message="start copy query", query={copy_query_log}')
            cursor.execute(copy_query)
            logging.info(f's3_to_redshift, message="finish copy query"')
        else:
            raise e
    except Exception as e:
        logging.error(e)
    finally:
        cursor.close()
        conn.close()
    logging.info(f's3_to_redshift, event=end, message="success"')


def file_to_db(source_s3_uri, schema, table):
    logging.info(f'file_to_db, event=start')
    source = construct_source_s3_detail(source_s3_uri)
    target = construct_target_s3_detail(schema, table)

    s3_client = boto3.client('s3', region_name=get_parameter('/s3/data/region'))

    if source['s3_file_extension'] == '.xlsx':
        logging.info(f'file_to_db, message="converting .xlsx file to .csv")')
        temp_csv_filepath = f"/tmp/{target['s3_filename']}_{time.time()}{target['s3_file_extension']}"
        df = pd.read_excel(source_s3_uri)
        df['source_filename'] = f"{source['s3_filename']}{source['s3_file_extension']}"
        df.to_csv(temp_csv_filepath, index=False)

        logging.info(f'file_to_db, message="uploaded .csv file to target s3")')
        s3_client.upload_file(temp_csv_filepath, target['s3_bucket'], target['s3_key'])

    elif source['s3_file_extension'] == '.csv':
        if source['s3_uri'] != target['s3_uri']:
            logging.info(f'file_to_db, message="moving .csv file from source s3 to target s3")')
            copy_source = {'Bucket': source['s3_bucket'], 'Key': source['s3_key']}
            s3_client.copy(copy_source, target['s3_bucket'], target['s3_key'])

    else:
        raise Exception(f"Source file type {source['s3_file_extension']} not supported")

    s3_to_redshift(target['s3_uri'], schema, table)
    logging.info(f'file_to_db, event=end, message="success"')


# if __name__ == '__main__':
#     file_to_db('s3://data.lake.mekarde2/externals/[Confidential] Mekari - Data Engineer Senior.xlsx',
#                'staging',
#                'transactions')
