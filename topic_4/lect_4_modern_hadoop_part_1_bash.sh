#!/bin/bash

# clean Hive

hive -e "DROP TABLE IF EXISTS users"
hive -e "DROP TABLE IF EXISTS instances"
hive -e "DROP TABLE IF EXISTS logs"

# clean HDFS

hdfs dfs -rm -R /jovyan/raw
hdfs dfs -rm -R /jovyan/users
hdfs dfs -rm -R /jovyan/instances
hdfs dfs -rm -R /jovyan/logs

# Data

hdfs dfs -mkdir /jovyan/raw
hdfs dfs -put ~/__DATA/IBDT_Spring_2023/topic_4/jhub_logs.csv /jovyan/raw/

# PySpark processing

pip install names
python lect_4_modern_hadoop_part_1_script.py

# Hive import

file_name=$(hdfs dfs -ls -C /jovyan/users | grep csv)
hive -e \
    "CREATE TABLE users ( \
        jh_email STRING, \
        jh_login STRING, \
        jh_name STRING) \
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
hive -e "LOAD DATA INPATH '${file_name}' OVERWRITE INTO TABLE users"

file_name=$(hdfs dfs -ls -C /jovyan/logs | grep csv)
hive -e \
    "CREATE TABLE logs ( \
        jh_timestamp TIMESTAMP, \
        jh_hub STRING, \
        jh_event_code INT, \
        jh_event_type STRING, \
        jh_log STRING, \
        jh_login STRING) \
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
hive -e "LOAD DATA INPATH '${file_name}' OVERWRITE INTO TABLE logs"

# Hive test

hive -S -e "SELECT * FROM users LIMIT 3" > result_sh.txt
hive -S -e "SELECT * FROM logs LIMIT 3" >> result_sh.txt

