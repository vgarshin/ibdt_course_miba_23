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
    
    # drop tables

    client = clickhouse_connect.get_client(
        host=access_ch['host'], 
        username=access_ch['user'], 
        password=access_ch['password'],
        port=access_ch['port'],
        verify='/tmp/YandexInternalRootCA.crt'
    )

    for table in ['users', 'logs', 'instances']:
        client.query(f'DROP TABLE IF EXISTS db1.{table}')
    