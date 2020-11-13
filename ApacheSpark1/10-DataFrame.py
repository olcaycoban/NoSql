from pyspark import SparkContext
import os

os.environ['HADOOP_HOME'] = "C:\\hd"

sc = SparkContext("local", "first app")

from pyspark.sql import SparkSession

# May take a little while on a local computer
spark = SparkSession.builder.appName("groupbyagg").getOrCreate()

df = spark.read.csv('data3.csv', inferSchema=True, header=True)

df.printSchema()

df.groupBy("country").show()
