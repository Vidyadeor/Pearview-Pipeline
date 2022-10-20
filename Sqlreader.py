import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
# /home/ubuntu/Pearview-Pipeline/jars/


key1 = "AKIAY6E3QNPJNIUJ334Q"
val1 = "7p4GidKLI+nIup9XzUpKIQc5r+9ExsmNMkyZxX9+"


def create_spark_session():
    try:
        spark = SparkSession \
           .builder.config("spark.jars", "/home/ubuntu/Pearview-Pipeline/jars/mysql-connector-java-8.0.23.jar,/home/ubuntu/Pearview-Pipeline/jars/hadoop-aws-3.2.3.jar,/home/ubuntu/Pearview-Pipeline/jars/aws-java-sdk-bundle-1.11.375.jar")\
           .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
           .config('spark.hadoop.fs.s3a.access.key', f'{key1}')\
           .config('spark.hadoop.fs.s3a.secret.key', f'{val1}')\
           .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')\
           .master("local").appName("Preview data pipeline")\
           .getOrCreate()
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", f"{key1}")
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", f"{val1}")

        return spark
    except Exception as e:
        print(e)

def read_from_mysql(table_name):
    try:
        df = spark.read \
             .format("jdbc") \
             .option("url", "jdbc:mysql://database-1.chptuk37dacd.ap-south-1.rds.amazonaws.com:3306/peardb") \
             .option("driver", "com.mysql.cj.jdbc.Driver") \
             .option("dbtable", table_name) \
             .option("user", "admin") \
             .option("password", "Satya123") \
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





if __name__ == "__main__":
    table_list = ["Student","courses","exam","score"]
    spark = create_spark_session()
    print(f'Spark Session Is : {spark}')
    for table in table_list:
        print(table)
        df = read_from_mysql(table)
        path ="s3a://prewiew-pear-raw-data/"+table
        write_to_s3(df,path)
        
