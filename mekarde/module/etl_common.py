import logging
import os
from pathlib import Path
from string import Template

import boto3
import redshift_connector

logging.basicConfig(level=logging.INFO)
filepath = os.path.dirname(os.path.abspath(__file__ + '/../')) + '/file'


def get_parameter(parameter_name):
    ssm_client = boto3.client('ssm', region_name='ap-southeast-1')
    response = ssm_client.get_parameter(
        Name=parameter_name,
        WithDecryption=True
    )
    return response['Parameter']['Value']


def get_s3_client():
    s3_client = boto3.client('s3', region_name=get_parameter('/s3/data/region'))
    return s3_client


def get_redshift_connection():
    return redshift_connector.connect(
        host=get_parameter('/redshift/host'),
        database=get_parameter('/redshift/db'),
        port=int(get_parameter('/redshift/port')),
        user=get_parameter('/redshift/user'),
        password=get_parameter('/redshift/password')
    )


def s3_to_redshift(s3_uri, schema_name, table_name, create_table_query, columns=None):
    logging.info(f's3_to_redshift, event=start, s3_uri={s3_uri}, schema_name={schema_name}, table_name={table_name}')

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
        columns='' if not columns else columns,
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
        if ('nonexistent table' in error_message or
                f'relation "{schema_name}.{table_name}" does not exist' in error_message):

            logging.info(f's3_to_redshift, message="start create table", query={create_table_query}')
            cursor.execute(create_table_query)
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
