import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging
from pyspark.sql.functions import col, count, when
import datetime
from datetime import date

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


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


# jdbc:redshift://pearview.cpettxwhj8e5.ap-south-1.redshift.amazonaws.com:5439/dev
logger.info('Succesfully executed')

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

    # passPercent_by_location = student_df.join(exam_df, exam_df.STUDENT_ID == student_df.STUDENT_ID) \
    # .groupBy(col("LOCATION")) \
    #  .withColumn("PASS_PERCENT", avg(col("RESULT")) * 100) \
    .orderBy(col("PASS_PERCENT"))
# passPercent_by_location.show()

write_to_redshift(npassPercent_by_locatio, "passPercent_by_location", url, username, password)
