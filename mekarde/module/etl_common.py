import boto3
import redshift_connector

def get_parameter(parameter_name):
    ssm_client = boto3.client('ssm', region_name='ap-southeast-1')
    response = ssm_client.get_parameter(
        Name=parameter_name,
        WithDecryption=True  # Set to True if the parameter is encrypted
    )
    return response['Parameter']['Value']

def get_redshift_connection():
    return redshift_connector.connect(
        host=get_parameter('/redshift/host'),
        database=get_parameter('/redshift/db'),
        port=int(get_parameter('/redshift/port')),
        user=get_parameter('/redshift/user'),
        password=get_parameter('/redshift/password')
    )