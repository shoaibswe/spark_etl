# # !pip install ipython-sql
import pandas as pd
import self as self
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
# from pyspark import SparkContext as sc
# sc.setLogLevel('ERROR')

engine = create_engine('postgresql://user:pass@host.rds.amazonaws.com:5432/dbname')
_in_mon=input("Enter Month")
_in_year=input("Enter Year")
df = pd.read_sql('SELECT * FROM partner_stg.kpi_achievement where month=%s and year=%s', engine,params=(_in_mon,_in_year))
df.to_csv('file4.csv',header=True, index=False)

df3 = pd.read_csv("file4.csv", header=0)
print(df3.head(5))

# df2 = pd.read_csv("file2.csv", header=0)
# # df3 = pd.read_csv("file3.csv", header=0)
#
# # importing sparksession from pyspark.sql module
# # creating sparksession and giving an app name
# spark = SparkSession.builder.appName('joinings').getOrCreate()
#
# df2Columns = ["id","name"]
# DF2d = spark.createDataFrame(data=df2, schema = df2Columns)
# df3Columns = ["id","name"]
# DF3d = spark.createDataFrame(data=df3, schema = df3Columns)
#
# # DF2d.printSchema()
# # DF2d.show(truncate=False)
#
# joined=DF2d.join(DF3d,
#                  DF2d.id ==  DF3d.id,
#                  "inner")
# joined.show()
