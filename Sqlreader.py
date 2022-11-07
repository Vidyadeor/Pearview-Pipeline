
#Author : Satya

import pyspark
from pyspark.sql import SparkSession
import datetime
from pytz import timezone 
from datetime import date
from cryptography.fernet import Fernet
import configparser
import numpy as np
from db_utils import DB
import os
# parse config data
cfg = configparser.ConfigParser()
cfg.read('config.ini')

access_key_encrypted = cfg['aws']['aws_access_key']
access_value_encrypted = cfg['aws']['aws_access_value']
secret_key_encrypted = cfg['aws']['aws_secret_key']
secret_value_encrypted = cfg['aws']['aws_secret_value']
mysql_user = cfg['mysql']['user']
mysql_pwd_key = cfg['mysql']['pwd_key']
mysql_password = cfg['mysql']['password']


# password decryption
def decrypt_value(key, value):
    try:
        fernet = Fernet(bytes(key, encoding='utf-8'))
        decrypted_value = fernet.decrypt(bytes(value, encoding='utf-8')).decode()
    except Exception as e:
        print('error while decryption')
        print(e)
    return decrypted_value

access_key = decrypt_value(access_key_encrypted, access_value_encrypted)
secret_key = decrypt_value(secret_key_encrypted, secret_value_encrypted)


# create spark session
def create_spark_session():
    try:
        spark = SparkSession \
           .builder.config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
           .config('spark.hadoop.fs.s3a.access.key', f'{access_key}')\
           .config('spark.hadoop.fs.s3a.secret.key', f'{secret_key}')\
           .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')\
           .master("local").appName("Preview data pipeline")\
           .getOrCreate()
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", f"{access_key}")
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", f"{secret_key}")
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", 'org.apache.hadoop.fs.s3a.S3AFileSystem')

        return spark
    except Exception as e:
        print(e)


# reading data tables from mysql
def read_from_mysql(table_name):
    try:
        password = decrypt_value(mysql_pwd_key, mysql_password)
        df = spark.read \
             .format("jdbc") \
             .option("url", "jdbc:mysql://database-1.chptuk37dacd.ap-south-1.rds.amazonaws.com:3306/pearview") \
             .option("driver", "com.mysql.cj.jdbc.Driver") \
             .option("dbtable", table_name) \
             .option("user", mysql_user) \
             .option("password", password) \
             .load() 
        return  df 
    except Exception as e:
        print(e)


def write_to_s3(df,path):
    df.write.mode("overwrite").option("header","true").csv(path)


def close_session(spark):
    spark.stop()


def orchestration():
    # Step 1
    try:
        table_list = ["student","courses","exam","score"]
        curr_date = date.today()
        records_processed = 0
        for table in table_list:
            print(table)
            df = read_from_mysql(table)
            records_processed+=df.count()
            print(f"dataframe {table} is{df.show()}")
            path =f"s3a://perviewdata/{curr_date}/"+table
            write_to_s3(df,path)
    except Exception as e:
        job_details['job_status'] = 'Failed'
        print(e)

    # Step 2
    try:
        # Updating Job details
        end_time = datetime.datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S.%f')
        job_details['records_processed'] = records_processed
        job_details['job_status'] = 'Completed'
        job_details['start_time'] = start_time
        job_details['end_time'] = end_time
        job_details['job_type'] = 'Data Ingestion'
        job_details['parameters'] = str({})
        job_details['jobId'] = jobId
        print(job_details)
        table = 'job_execution_log'
        query, params = DB.insert_dict(job_details, table)
        res = DB.execute(query, params)
    except Exception as e:
        print(f'Error while updating job_details {e}')


if __name__ == "__main__":

    job_details = {}
    start_time = datetime.datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S.%f')
    spark = create_spark_session()
    global jobId 
    jobId = spark.sparkContext.applicationId
    print(f'Spark Session Is : {spark.sparkContext.applicationId}')
    orchestration()
    close_session(spark)
