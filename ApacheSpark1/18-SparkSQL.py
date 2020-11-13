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
sel_df=df.select("name","age","gender","country")

#groupbys
sel_df.groupBy("country").count().show()
sel_df.groupBy("country").max().show()
sel_df.groupBy("country").avg("age").show()


