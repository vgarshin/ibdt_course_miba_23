import os
import sys
import json
import boto3
import clickhouse_connect

AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

def access_data(file_path):
    with open(file_path) as file:
        access_data = json.load(file)
    return access_data

def handler(event, context):

    # get credentials from object storage

    DATA_BUCKET = 'ibdt-jhub-logs'

    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        endpoint_url='https://storage.yandexcloud.net'
    )
    s3.download_file(DATA_BUCKET, 'YandexInternalRootCA.crt', '/tmp/YandexInternalRootCA.crt')
    s3.download_file(DATA_BUCKET, 'access_ch.json', '/tmp/access_ch.json')
    access_ch = access_data('/tmp/access_ch.json')
    
    # create pulse table

    client = clickhouse_connect.get_client(
        host=access_ch['host'], 
        username=access_ch['user'], 
        password=access_ch['password'],
        port=access_ch['port'],
        verify='/tmp/YandexInternalRootCA.crt'
    )

    client.query(f'DROP TABLE IF EXISTS db1.pulse')
    
    parameters = {
        'table1': 'logs',
        'table2': 'users',
        'key': 'login',
    }
    client.query(
        "CREATE TABLE db1.pulse ( \
            timestamp String, \
            event_code Int32, \
            name String \
        ) ENGINE = MergeTree ORDER BY timestamp AS \
        SELECT ls.timestamp, ls.event_code, us.name FROM db1.{table1:Identifier} AS ls \
        LEFT JOIN db1.{table2:Identifier} AS us ON ls.{key:Identifier} = us.{key:Identifier}",
        parameters=parameters
    )
