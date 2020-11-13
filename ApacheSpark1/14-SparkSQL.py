from pyspark.shell import sqlContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("First Spark Sql Exam").getOrCreate()

df = spark.read.csv('data3.csv', inferSchema=True, header=True)

sqlContext.registerDataFrameAsTable(df, "table1")
df = sqlContext.sql("SELECT *from table1")

df=df.withColumn('name2',df['name'])
df.show(df.count(),False)