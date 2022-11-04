
#Author : Satya

import pyspark
from pyspark.sql import SparkSession
import datetime
from datetime import date
from cryptography.fernet import Fernet
import configparser

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


print(access_value_encrypted)

# password decryption
def decrypt_value(key, value):
    fernet = Fernet(key)
    decrypted_value = fernet.decrypt(value).decode()
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
    try:
        table_list = ["student","courses","exam","score"]
        curr_date = date.today()
        for table in table_list:
            print(table)
            df = read_from_mysql(table)
            print(f"dataframe {table} is{df.show()}")
            path =f"s3a://perviewdata/{curr_date}/"+table
            write_to_s3(df,path)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    spark = create_spark_session()
    print(f'Spark Session Is : {spark}')
    orchestration()
    close_session(spark)