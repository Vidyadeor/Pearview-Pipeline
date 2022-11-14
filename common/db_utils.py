
import mysql.connector
import numpy as np


ENDPOINT="database-1.chptuk37dacd.ap-south-1.rds.amazonaws.com"
PORT="3306"
USER="admin"
PASSWORD = "Admin123"
DBNAME="pearview"

class DB():

    def execute(query, params):
        try:
            conn =  mysql.connector.connect(host=ENDPOINT, user=USER, passwd=PASSWORD, port=PORT, database=DBNAME)
            cursor = conn.cursor()
            #query_results = cur.fetchall()
            #print(query_results)
            print(query,params)
            cursor.execute(query, params)
            conn.commit()
            return True
        except Exception as e:
            print("Database connection failed due to {}".format(e))  
            return e        
        

    def insert_dict(data, table):
        """
        Insert dictionary into a SQL database table.

        Args:
            data (DataFrame): The DataFrame that needs to be write to SQL database.
            table (str): The table in which the rcords should be written to.

        Returns:
            (bool) True is succesfully inserted, else false.
        """
        print(f'Inserting dictionary data into {table}...')
        print(f'Data:\n{data}')

        try:
            column_names = []
            params = []

            for column_name, value in data.items():
                column_names.append(f'{column_name}')
                params.append(value)

            print(f'Column names: {column_names}')
            print(f'Params: {params}')

            columns_string = ', '.join(column_names)
            param_placeholders = ', '.join(['%s'] * len(column_names))

            query = f'INSERT INTO {table} ({columns_string}) VALUES ({param_placeholders})'
            params = tuple(params)
            params = [int(i) if isinstance(i, np.int64) else i for i in params]
            return query, params
        except:
            print('Error inserting data.')
            return False
