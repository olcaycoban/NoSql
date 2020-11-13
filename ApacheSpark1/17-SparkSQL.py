from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Third Spark Sql Exam").getOrCreate()

#structtype
schema = StructType([
       StructField("name", StringType(), True),
       StructField("surname", StringType(), True),
       StructField("gender", StringType(), True),
       StructField("age", IntegerType(), True),
       StructField("country", StringType(), True)])

df = spark.read.csv('data5.csv', schema=schema, header=True)

#select
sel_df=df.select("name","age","country")
#sel_df.show(sel_df.count(),False)

#filter
chinese=sel_df.filter(sel_df["country"]=="Serbia")
chinese.show(chinese.count(),False)