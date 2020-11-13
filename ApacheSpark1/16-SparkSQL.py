from pyspark.shell import sqlContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Second Spark Sql Exam").getOrCreate()

schema = StructType([
       StructField("name", StringType(), True),
       StructField("surname", StringType(), True),
       StructField("gender", StringType(), True),
       StructField("age", IntegerType(), True),
       StructField("country", StringType(), True)])

df = spark.read.csv('data5.csv', schema=schema, header=True)
df.show(df.count(),False)
df.printSchema()

