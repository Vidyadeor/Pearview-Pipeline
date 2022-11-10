
#Author : Vidya

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,count
import datetime
from datetime import date

def create_spark_session():
        spark = SparkSession \
           .builder.config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
           .config('spark.hadoop.fs.s3a.access.key', 'XXXXXX')\
           .config('spark.hadoop.fs.s3a.secret.key', 'XXXXXXX')\
           .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')\
           .master("local").appName("Preview data pipeline")\
           .getOrCreate()
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "XXXXXXX")
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "XXXXXX")

        return spark


def read_from_s3(path):
        df = spark.read.format("csv").option("header",True).option("inferSchema",True).load(path)
        return df
        

def write_to_redshift(df,table_name,url,username,password):
       df.write.format("jdbc").option("driver","com.amazon.redshift.jdbc42.Driver").option("dbtable", table_name).option("url", url).option("user", username).option("password", password).mode("append").save()


if __name__ == "__main__":
    table_list = ["Student","courses","exam","score"]
    spark = create_spark_session()
    curr_date = date.today()
    path =f"s3a://perviewdata/{curr_date}/"
    url = "jdbc:redshift://pearview.cpettxwhj8e5.ap-south-1.redshift.amazonaws.com:5439/finaldata"
    username ="XXXXX"
    password = "XXXXX"
    
    student_df = read_from_s3(path+"Student")
    course_df = read_from_s3(path+"courses")
    score_df = read_from_s3(path+"score")
    exam_df = read_from_s3(path+"exam")
    
    no_of_course = student_df.join(course_df,student_df.STUDENT_ID == course_df.STUDENT_ID).groupBy(col("COURSE_NAME")).count().withColumnRenamed("count","No_of_Student")
    no_of_course.show()
    write_to_redshift(no_of_course,"no_of_course",url,username,password)
    
    
    avg_marks_per_course = score_df.join(course_df,score_df.COURSE_ID == course_df.COURSE_ID).groupBy(col("COURSE_NAME")).avg("SCORE").withColumnRenamed("avg(SCORE)","Average_Marks")
    avg_marks_per_course.show()
    write_to_redshift(avg_marks_per_course,"avg_marks_per_course",url,username,password)
    
    
    
        
    







