from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("groupbyagg").getOrCreate()

df = spark.read.csv('data4.csv', inferSchema=True, header=True)

df.printSchema()

df.groupBy("department").pivot("state").sum("salary").show()