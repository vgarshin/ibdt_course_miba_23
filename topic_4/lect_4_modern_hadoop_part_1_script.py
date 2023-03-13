#!/usr/bin/env python
# coding: utf-8

import os
import names
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql import functions as F

# Spark initialization

conf = SparkConf().set('spark.master', 'local[*]')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
spark

# Data read 

sdf = spark.read.csv(
    f'/jovyan/raw/jhub_logs.csv',
    sep=';', 
    header=True,
    multiLine=True, # if you have `\n` symbols
    escape="\""
)

# Data processing

def row_info(rin):
    """
    Extracts names of:
      - docker image
      - id of the Jupyter application
      - name of the host, where Jupyter runs
    
    """
    img = rin[rin.find('container_image='):].split('\'')[1]
    hub = rin[rin.find('pod_name='):].split('\'')[1]
    host = rin[rin.find('host='):].split('\'')[1]
    return img, hub, host

def sq_brackets(sin):
    """
    Split log string amd extracts:
      - timestamp of the event
      - name of application
      - type of logs
      - code of event
      - description
    
    """
    try:
        s = sin.split('[', 1)[1].split(']')[0]
        msg = sin[len(s) + 2 :].strip()
        s = s.split()
        head = s[0]
        ts = ' '.join(s[1:3])
        svc = s[3]
        typ = s[4].split(':')[0]
        code = s[4].split(':')[1]
    except:
        head, ts, svc, typ, code = '', '', '', '', ''
        msg = sin
    return head, ts, svc, typ, code, msg

udf_row_info = udf(row_info, ArrayType(StringType()))
sdf = sdf.withColumn('kubernetes_msg', udf_row_info('kubernetes'))
sdf = sdf.select(
    'date',
    F.col('kubernetes_msg')[0].alias('img'),
    F.col('kubernetes_msg')[1].alias('hub'),
    F.col('kubernetes_msg')[2].alias('host'),
    'log',
    'stream',
    'time'
)

udf_sq_brackets = udf(sq_brackets, ArrayType(StringType()))
sdf = sdf.withColumn('log_msg', udf_sq_brackets('log'))
sdf = sdf.select(
    'img',
    'hub',
    'host',
    F.col('log_msg')[0].alias('head'),
    F.col('log_msg')[1].alias('timestamp'),
    F.col('log_msg')[2].alias('service'),
    F.col('log_msg')[3].alias('event_type'),
    F.col('log_msg')[4].alias('event_code'),
    F.col('log_msg')[5].alias('message')
)

def parce_users_activities(code, msg):
    """
    Ugly function.
    
    You may use dictionary to make it
    more pythonic or something else.
    
    """
    if code == '43':
        user = msg.split()[-1]
        log = 'logged out'
    elif code == '757':
        user = msg.split()[-1]
        log = 'logged in'
    elif code == '402':
        user = msg.split()[0]
        log = 'pending spawn'
    elif code == '1875':
        user = msg.split()[4].replace('claim-', '').replace(',', '')
        log = 'attempt to create pvc with timeout'
    elif code == '1887':
        user = msg.split()[1].replace('claim-', '')
        log = 'pvc already exists'
    elif code == '1840':
        user = msg.split()[4].replace('jupyter-', '').replace(',', '')
        log = 'attempting to create pod with timeout'
    elif code == '1344':
        user = msg.split('/')[3]
        log = 'failing suspected api request to not-running server'
    elif code == '380':
        user = msg.split()[3]
        log = 'previous spawn failed'
    elif code == '567':
        user = msg.split('/')[4]
        log = 'stream closed while handling '
    elif code == '681':
        user = msg.split()[0].replace('\'s', '')
        log = 'server failed to start'
    elif code == '1997':
        user = msg.split('-')[-1]
        log = 'deleting pod'
    elif code == '689':
        user = msg.split()[3].replace('\'s', '')
        log = 'unhandled error starting with timeout'
    elif code == '1961' or code == '2044':
        user = msg.split()[1].replace('jupyter-', '')
        log = 'restarting pod reflector'
    elif code == '257':
        user = msg.split()[2]
        log = 'adding user to proxy'
    elif code == '664':
        user = msg.split()[1]
        log = 'server is ready'
    elif code == '61' or code == '85':
        user = msg.split()[3]
        log = 'spawning sever with advanced configuration option'
    elif code == '1143':
        user = msg.split()[1].replace(':', '')
        log = 'server is slow to stop'
    elif code == '2077':
        user = msg.split()[0]
        log = 'still running'
    elif code == '167':
        user = msg.split()[1]
        log = 'server is already active'
    elif code == '1067' or code == '2022':
        user = msg.split()[1]
        log = 'user server stopped with exit code 1'
    elif code == '1857':
        user = msg.split()[3].replace('jupyter-', '').replace(',', '')
        log = 'found existing pod and attempting to kill'
    elif code == '1861':
        user = msg.split()[2].replace('jupyter-', '').replace(',', '')
        log = 'killed pod and will try starting singleuser pod again'
    elif code == '738':
        user = msg.split()[0].replace(',', '').replace('\'s', '')
        log = 'server never showed up and giving up'
    elif code == '2069':
        user = msg.split()[0].replace(',', '')
        log = 'user does not appear to be running and shutting it down'  
    elif code == '148':
        user = msg.split()[-1]
        log = 'user is running'
    elif code == '1415':
        user = msg.split()[-1]
        log = 'admin requesting spawn on behalf'
    elif code == '1437':
        user = msg.split()[5].replace(',', '')
        log = 'user requested server which user do not own'
    elif code == '626':
        user = msg.split()[1]
        log = 'server is already started'
    elif code == '2085':
        user = msg.split()[0]
        log = 'server appears to have stopped while the hub was down'
    else:
        user, log = '', ''
    return user, log

udf_parce_users_activities = udf(parce_users_activities, ArrayType(StringType()))
sdf = sdf.withColumn('user_act', udf_parce_users_activities(sdf['event_code'], sdf['message']))
sdf = sdf.select(
    F.col('timestamp').alias('jh_timestamp'),
    F.col('hub').alias('jh_hub'),
    F.col('img').alias('jh_img'),
    F.col('host').alias('jh_host'),
    F.col('event_code').alias('jh_event_code'),
    F.col('event_type').alias('jh_event_type'),
    F.col('user_act')[1].alias('jh_log'),
    F.col('user_act')[0].alias('jh_user')
)
sdf = sdf.filter(sdf.jh_user != '')
sdf = sdf.withColumn(
    'jh_timestamp',
    F.to_timestamp("jh_timestamp", "yyyy-MM-dd HH:mm:ss.SSS")
)

# Users table

logins = sdf.select('jh_user').distinct().collect()
logins = [list(x)[0] for x in logins]
users = []
for login in logins:
    user = {}
    user['login'] = login
    user['name'] = names.get_full_name()
    user['email'] = login + '@gsom.spbu.ru'
    users.append(user)
rdd = sc.parallelize([users])
sdf_users = spark.read.json(rdd)
sdf_users.coalesce(1).write.csv('/jovyan/users')

# Instances table

sdf_instances = sdf.select(
    'jh_hub',
    'jh_img',
    'jh_host'
)
sdf_instances = sdf_instances.dropDuplicates()
sdf_instances.coalesce(1).write.csv('/jovyan/instances')

# Events table

# HOME ASSIGNMENT

# Your code will be here
# create a table `events` with columns `event_code`, `event_type`, `log`
# drop duplicates and save it to CSV file (to HDFS) to import to database later 

# Logs table

# NOTE that it is an example
# you will need to keep only `event_code` column as a key
# and remove `event_type` and `log` columns
# for data normalization

sdf_logs = sdf.select(
    'jh_timestamp',
    'jh_hub',
    'jh_event_code',
    'jh_event_type',   # to be removed
    'jh_log',          # to be removed
    'jh_user'
)
sdf_logs.coalesce(1).write.option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SS").csv('/jovyan/logs')

# End of all

spark.stop()