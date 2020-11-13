from pyspark import SparkContext
from pyspark.sql.functions import countDistinct
import os

os.environ['HADOOP_HOME'] = "C:\\hd"

sc = SparkContext("local", "first app")

# read input text file to RDD
lines = sc.textFile(r"C:\Users\olcay\PycharmProjects\ApacheSpark1\data2.csv")


# map lines to n_words
n_words = lines.groupByKey().map(lambda line: line.split(","))


for i in n_words:
    print()