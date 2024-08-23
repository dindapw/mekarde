import logging
import os
import time

import pandas as pd

from mekarde.module.etl_common import get_parameter, get_s3_client, s3_to_redshift

filepath = os.path.dirname(os.path.abspath(__file__ + '/../')) + '/file'
logging.basicConfig(level=logging.INFO)


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


def file_to_db(source_s3_uri, schema, table):
    logging.info(f'file_to_db, event=start')
    source = construct_source_s3_detail(source_s3_uri)
    target = construct_target_s3_detail(schema, table)

    s3_client = get_s3_client()

    if source['s3_file_extension'] == '.xlsx':
        logging.info(f'file_to_db, message="converting .xlsx file to .csv")')
        temp_csv_filepath = f"/tmp/{target['s3_filename']}_{time.time()}{target['s3_file_extension']}"
        df = pd.read_excel(source_s3_uri)
        df['source_filename'] = f"{source['s3_filename']}{source['s3_file_extension']}"
        df.to_csv(temp_csv_filepath, index=False)
        header = df.columns.tolist()

        logging.info(f'file_to_db, message="uploaded .csv file to target s3")')
        s3_client.upload_file(temp_csv_filepath, target['s3_bucket'], target['s3_key'])

    elif source['s3_file_extension'] == '.csv':
        if source['s3_uri'] != target['s3_uri']:
            logging.info(f'file_to_db, message="moving .csv file from source s3 to target s3")')
            copy_source = {'Bucket': source['s3_bucket'], 'Key': source['s3_key']}
            s3_client.copy(copy_source, target['s3_bucket'], target['s3_key'])
        df = pd.read_csv(target['s3_uri'], nrows=0)
        header = df.columns.tolist()

    else:
        raise Exception(f"Source file type {source['s3_file_extension']} not supported")

    columns = ', '.join([f'{col_name}' for col_name in transform_column_name(header)])
    columns = f'({columns})'

    columns_create_table = ', '.join([f'{col_name} VARCHAR(max)' for col_name in transform_column_name(header)])
    columns_create_table += ', load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
    create_table_query = f'CREATE TABLE IF NOT EXISTS {schema}.{table} ({columns_create_table});'

    s3_to_redshift(target['s3_uri'], schema, table, create_table_query, columns)
    logging.info(f'file_to_db, event=end, message="success"')


if __name__ == '__main__':
    file_to_db('s3://data.lake.mekarde2/externals/[Confidential] Mekari - Data Engineer Senior.xlsx',
               'staging',
               'transactions')
