from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("groupbyagg").getOrCreate()

#structtype
schema = StructType([
       StructField("name", StringType(), True),
       StructField("surname", StringType(), True),
       StructField("email", StringType(), True),
       StructField("country", StringType(), True),
       StructField("price", IntegerType(), True),
       StructField("product", StringType(), True)])

df = spark.read.json('products.json',schema=schema, multiLine=True)
#df.show(df.count(),False)

#df.groupBy("country","product").sum("price").show(df.count(),False)
#df.groupBy("country","product").count().show(df.count(),False)

df.groupBy("country","product").avg().show(df.count(),False)