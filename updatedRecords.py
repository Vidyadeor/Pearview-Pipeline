from pyspark.sql import SparkSession
from datetime import datetime


def create_spark_session():
    spark = SparkSession \
        .builder \
        .appName("Word Count") \
        .config("spark.driver.extraClassPath", "mysql-connector-java-8.0.23.jar") \
        .getOrCreate()
    return spark


def read_from_mysql(table_name):
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/peraview") \
        .option("driver", "com.mysql.jdbc.Driver") \
        .option("dbtable", table_name) \
        .option("user", "root") \
        .option("password", "root123") \
        .load()
    return df


# df.show(10)


if __name__ == "__main__":
    # table_list = ["student","courses","exam","score"]
    spark = create_spark_session()
    df = read_from_mysql("student")
    # df.show(10)
    df.createOrReplaceTempView('records')
    # data = spark.sql(f"select * from records")
data = spark.sql(f"select * from records where UPDATED_DATE between '2022-11-03 08:32:23' and '2022-11-06 05:56:25'")
data.show(10)

start_time = str(input("enter start date:"))
end_time = str(input("enter end date:"))
date_format = '%Y-%m-%d %H:%M:%S'

# try:
# start_date = datetime.strptime(start_time, date_format)
# except ValueError:
# print("Incorrect start data format, should be YYYY-MM-DD HH:MM:MM")

# try:
# end_date = datetime.strptime(end_time, date_format)
# except ValueError:
# print("Incorrect end data format, should be YYYY-MM-DD HH:MM:SS")
# data = spark.sql("select * from records where UPDATED_DATE between '{start_date}' and '{end_date}'")
data.show(10)

# 2022-11-03 08:32:23
