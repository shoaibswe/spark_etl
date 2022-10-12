# # !pip install ipython-sql
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
import self as self
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
# from pyspark import SparkContext as sc
# sc.setLogLevel('ERROR')

engine = create_engine('postgresql://user:pass@host.rds.amazonaws.com:5432/dbname')

df = pd.read_sql('SELECT * FROM iris_dev.test', engine)
df.to_csv('file3.csv',header=True, index=False)

df2 = pd.read_csv("file2.csv", header=0)
df3 = pd.read_csv("file3.csv", header=0)

# importing sparksession from pyspark.sql module
# creating sparksession and giving an app name
spark = SparkSession.builder.appName('joinings').getOrCreate()

df2Columns = ["id","name"]
DF2d = spark.createDataFrame(data=df2, schema = df2Columns)
df3Columns = ["id","name"]
DF3d = spark.createDataFrame(data=df3, schema = df3Columns)

# DF2d.printSchema()
# DF2d.show(truncate=False)

joined=DF2d.join(DF3d,
                 DF2d.id ==  DF3d.id,
                 "inner")
joined.show()
