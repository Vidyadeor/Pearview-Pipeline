
#Author : Vidya

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,count
import datetime
from datetime import date
from cryptography.fernet import Fernet
import configparser

cfg = configparser.ConfigParser()
cfg.read('config.ini')

access_key_encrypted = cfg['aws']['aws_access_key']
access_value_encrypted = cfg['aws']['aws_access_value']
secret_key_encrypted = cfg['aws']['aws_secret_key']
secret_value_encrypted = cfg['aws']['aws_secret_value']

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



def read_from_s3(path):
    df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(path)
    return df


def write_to_redshift(df, table_name, url, username, password):
    df.write.format("jdbc"). \
        option("url", url). \
        option("dbtable", table_name). \
        option("user", username). \
        option("password", password). \
        mode('append').save()


if __name__ == "__main__":
    table_list = ["student", "courses", "exam", "score"]
    curr_date = date.today()
    path = f"s3a://perviewdata/{curr_date}/"
    url = "jdbc:redshift://pearview.cpettxwhj8e5.ap-south-1.redshift.amazonaws.com:5439/finaldata"
    username = "admin"
    password = "Admin123"

    student_df = read_from_s3(path + "student")
    course_df = read_from_s3(path + "courses")
    score_df = read_from_s3(path + "score")
    exam_df = read_from_s3(path + "exam")

    no_of_course = student_df.join(course_df, student_df.STUDENT_ID == course_df.STUDENT_ID).groupBy(
        col("COURSE_NAME")).count().withColumnRenamed("count", "No_of_Student")
    write_to_redshift(no_of_course, "no_of_course", url, username, password)

    avg_marks_per_course = score_df.join(course_df, score_df.COURSE_ID == course_df.COURSE_ID).groupBy(
        col("COURSE_NAME")).avg("SCORE").withColumnRenamed("avg(SCORE)", "Average_Marks")
    avg_marks_per_course.show()
    write_to_redshift(avg_marks_per_course, "avg_marks_per_course", url, username, password)

    
    
    
        
    







