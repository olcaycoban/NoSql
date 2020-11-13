from pyspark.shell import sqlContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("First Spark Sql Exam").getOrCreate()

df = spark.read.csv('data3.csv', inferSchema=True, header=True)

df.printSchema()
"""
def f(x):
    print(x)
df.foreach(f)
"""
"""
llist=df.collect()

for i in llist:
    print(i)
"""

sqlContext.registerDataFrameAsTable(df, "table1")
df2 = sqlContext.sql("SELECT *from table1 where country='Sweden'")
#df2.collect()
#print(df2.head(5))

df2.show()
df2.select('name').show()

