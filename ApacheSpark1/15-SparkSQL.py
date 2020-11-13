from pyspark.shell import sqlContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Second Spark Sql Exam").getOrCreate()

df = spark.read.csv('data5.csv', inferSchema=True, header=True)

sqlContext.registerDataFrameAsTable(df, "table1")
df = sqlContext.sql("SELECT *from table1")

df=df.withColumn('age',df['age']+30)
df.show(df.count(),False)