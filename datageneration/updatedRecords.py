import configparser
from pyspark.sql import SparkSession
from cryptography.fernet import Fernet
from datetime import datetime
import sys

cfg = configparser.ConfigParser()
cfg.read('config.ini')

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

def create_spark_session():
    spark = SparkSession\
            .builder\
            .appName("Record_Count")\
            .getOrCreate()
    return spark
def read_from_mysql(query):
        password = decrypt_value(mysql_pwd_key, mysql_password)
        df = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:mysql://database-1.chptuk37dacd.ap-south-1.rds.amazonaws.com:3306/pearview") \
            .option("driver", "com.mysql.jdbc.Driver") \
            .option("dbtable", query) \
            .option("user", mysql_user) \
            .option("password", password) \
            .load()
        return  df
def date_check(dt):
    try:
        date_format = '%Y-%m-%d %H:%M:%S'
        check = datetime.strptime(dt, date_format)
    except ValueError:
        print(f"Incorrect START/END data format, should be YYYY-MM-DD HH:MM:MM")
        raise ValueError(f"Incorrect START/END data format, should be YYYY-MM-DD HH:MM:MM")

if __name__ == "__main__":
    #table_list = ["student","courses","exam","score"]
     start_time = sys.argv[1]
     end_time = sys.argv[2]
     date_check(start_time)
     date_check(end_time)
    #print(sys.argv)
     spark = create_spark_session()
     table_list = ["student"]
     for table in table_list:
        query = f"(select * from {table} where UPDATED_DATE between '{start_time}' and '{end_time}') tbl"
        data = read_from_mysql(query)
        data.show()
