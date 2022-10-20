


from pyspark.sql import SparkSession

def create_spark_session():
        spark = SparkSession \
           .builder.config("spark.jars", "/home/pragatisalunke/mysql-connector-java-8.0.23.jar,/home/pragatisalunke/hadoop-aws-3.2.3.jar,/home/pragatisalunke/aws-java-sdk-bundle-1.11.375.jar")\
           .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
           .config('spark.hadoop.fs.s3a.access.key', 'XXXXXXXX')\
           .config('spark.hadoop.fs.s3a.secret.key', 'XXXXXXXX')\
           .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')\
           .master("local").appName("Preview data pipeline")\
           .getOrCreate()
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "XXXXX")
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "XXXXX")

        return spark


def read_from_mysql(table_name):
        df = spark.read \
             .format("jdbc") \
             .option("url", "jdbc:mysql://localhost:3306/peardb") \
             .option("driver", "com.mysql.jdbc.Driver") \
             .option("dbtable", table_name) \
             .option("user", "root") \
             .option("password", "root123") \
             .load() 
        return  df 
        
def write_to_s3(df,path):
        df.write.mode("overwrite").csv(path)
        
def read_from_s3():
        df.write.mode("overwrite").csv(path)
        

def write_to_redshift():
        pass





if __name__ == "__main__":
    table_list = ["Student","courses","exam","score"]
    spark = create_spark_session()
    for table in table_list:
        df = read_from_mysql(table)
        path ="s3a://prewiew-pear-raw-data/"+table
        write_to_s3(df,path)
        
    







