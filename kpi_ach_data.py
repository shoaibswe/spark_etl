import psycopg2
import pandas as pd
import os

EXTRACTED_LOC = "output_files"


def get_connection(host, port, database, user, password):
    connection = None
    try:
        connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    except Exception as e:
        raise (e)

    return connection


host = 'host.rds.amazonaws.com'
port = '5432'
database = 'ecrm2_dev'
user = 'shuvo'
password = '1234'
connection = get_connection(
    host=host,
    port=port,
    database=database,
    user=user,
    password=password
)

if __name__ == "__main__":
    # pass
    try:
        orders_cursor = connection.cursor()

        cursor = connection.cursor()
        query = "SELECT * FROM iris_dev.test"
        cursor.execute(query)
        all_rows = cursor.fetchall()
        x = (list(all_rows))
        # for index, row in all_rows.x():
        #     file_name = row['Name']+".csv"  #Change the column name accordingly
        df = pd.DataFrame(x)
        # print(df)
        # saving the dataframe
        df.to_csv('file1.csv')
    except Exception as e:
        print("An Error Occured: ", e)

#
# dta_local = pd.read_csv('file1.csv')
# # print(dta_local)
# dta_local.head(1)