from pyspark import SparkContext
import os

os.environ['HADOOP_HOME'] = "C:\\hd"
sc = SparkContext("local", "first app")

test_array=["elasticsearch","big data","MongoDB","Spark","MongoDB"]
data=sc.parallelize(test_array)

print(data.first())

num_mongo = data.filter(lambda s: 'MongoDB' in s).count()
print(num_mongo)