#!/usr/bin/env python
# coding: utf-8

import os
import sys
import json
import boto3
import zipfile
import tarfile
from tqdm import tqdm
from boto3.s3.transfer import TransferConfig

def s3_client(access_file):
    with open(access_file) as file:
        access_data = json.load(file)
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        aws_access_key_id=access_data['aws_access_key_id'],
        aws_secret_access_key=access_data['aws_secret_access_key'],
        endpoint_url='http://storage.yandexcloud.net'
    )
    return s3

def list_buckets(s3):
    bucket_response = s3.list_buckets()
    buckets = bucket_response["Buckets"]
    print('buckets:', buckets)
    
def get_loaded_objects(s3, bucket, upfolder, verbose=False):
    s3_result = s3.list_objects_v2(Bucket=bucket, Prefix=upfolder)
    loaded = []
    if 'Contents' in s3_result.keys():
        for key in s3_result['Contents']:
            loaded.append(key['Key'])
    else:
        loaded = []
    if verbose: print(f'loaded: {len(loaded)}')
    while s3_result['IsTruncated']:
        continuation_key = s3_result['NextContinuationToken']
        s3_result = s3.list_objects_v2(
            Bucket=bucket, 
            Prefix=upfolder, 
            ContinuationToken=continuation_key
        )
        for key in s3_result['Contents']:
            loaded.append(key['Key'])
        if verbose: print(f'loaded: {len(loaded)}')
    return loaded

def copy_b2b(bucket_old, bucket_new, s3_old, s3_new):
    all_objects_old = get_loaded_objects(
        s3_old, 
        bucket=bucket_old, 
        upfolder='', 
        verbose=False
    )
    all_objects_new = get_loaded_objects(
        s3_new, 
        bucket=bucket_new, 
        upfolder='', 
        verbose=False
    )
    print('total objects (from):', len(all_objects_old))
    print('total objects (to):', len(all_objects_new))
    print('objects to copy:', len(all_objects_old) - len(all_objects_new))
    for obj in tqdm(all_objects_old, desc='copy'):
        if obj in all_objects_new:
            continue
        try:
            get_object_response = s3_old.get_object(
                Bucket=bucket_old, 
                Key=obj
            )
            s3_new.put_object(
                Bucket=bucket_new,
                Key=obj,
                Body=get_object_response['Body'].read()
            )
        except Exception as e:
            print('ERROR -', obj, '-', e)

def main():
    """
    Main copy function. Takes sys.args to control load and process data in a form:
      $ python <script_name> <credentials_from> <credentials_to> <bucket_name_from> <bucket_name_to> 

        :credentials_from: credentials for S3 storage to copy from
        :credentials_to: credentials for S3 storage to copy to
        :bucket_name_from: name of bucket to copy all files from
        :bucket_name_to: name of bucket to copy all files to
        
    if no <bucket_name_from> <bucket_name_to> are given then script prints only buckets' names 
    from S3 stirages with credentials provided
    
    """
    access_s3_old = sys.argv[1]
    access_s3_new = sys.argv[2]
    s3_old = s3_client(access_s3_old)
    s3_new = s3_client(access_s3_new)
    if len(sys.argv) == 3:
        list_buckets(s3_old)
        list_buckets(s3_new)
    else:
        bucket_old = sys.argv[3]
        bucket_new = sys.argv[4]
        copy_b2b(
            bucket_old=bucket_old, 
            bucket_new=bucket_new, 
            s3_old=s3_old, 
            s3_new=s3_new
        )
        
if __name__ == '__main__':
    main()
