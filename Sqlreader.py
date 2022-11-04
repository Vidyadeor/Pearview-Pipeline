
#Author : Satya

import pyspark
from pyspark.sql import SparkSession
import datetime
from datetime import date
from cryptography.fernet import Fernet


encry_aceess_key = b'gAAAAABjYkX_U8a1_xBmVziWAqZFnxjEaHHUo0me3kg6-CM_S-6izl0vKZ-HRAjomKK5ZMWltamvEBbPytw3xkvTLrx3LVRRYB8BHACOgfxMVvrNHeIKg3Q='
key1 = b'QqiMVLgR0EQ_8RtOkSTGTVdA9OwMgmIOPY5_5_q9Kdg='
fernet = Fernet(key1)
access_key = fernet.decrypt(encry_aceess_key).decode()

encry_secret_key = b'gAAAAABjYkfCzsn3NvELfTBJnWEbja4EQjQpDeo4mq4KBDTYl6xOqrgnMa_uCQPyKHTp8zIphfuY4oa7K0e34Qp_cF2FZRc0BeQiLdnMibp5OmPDAjJSDY1Trecz3HdOQF2pbenB8rgj'
key2 = b'3Z1g5reKuz15Kg24io2mBraVQ-yOwMHPjNQdVjaQ5Ec='
fernet = Fernet(key2)
secret_key = fernet.decrypt(encry_secret_key).decode()


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


def read_from_mysql(table_name):
    try:

        pwd_key = b'gAAAAABjYklL4Kj4nBzI1OXxZT7sHGMM_o7WfsQNEINLE1xbv348Sze44oo3y4l_aNX0510lHbnhmcmF9V3FojdwIsXXtcSE8A=='
        key3 = b't-OfylTkIPkrf8s3CIwN77wcgSppXuhI9btLJHO3yv8='
        fernet = Fernet(key3)
        password = fernet.decrypt(pwd_key).decode()

        df = spark.read \
             .format("jdbc") \
             .option("url", "jdbc:mysql://database-1.chptuk37dacd.ap-south-1.rds.amazonaws.com:3306/pearview") \
             .option("driver", "com.mysql.cj.jdbc.Driver") \
             .option("dbtable", table_name) \
             .option("user", "admin") \
             .option("password", password) \
             .load() 
        return  df 
    except Exception as e:
        print(e)


def write_to_s3(df,path):
    df.write.mode("overwrite").csv(path)

        
def read_from_s3():
    df.write.mode("overwrite").csv(path)
        

def write_to_redshift():
    pass

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
        close_session(spark)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    spark = create_spark_session()
    print(f'Spark Session Is : {spark}')
    orchestration()
