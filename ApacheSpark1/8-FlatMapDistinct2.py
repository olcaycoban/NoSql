from pyspark import SparkContext
import os

os.environ['HADOOP_HOME'] = "C:\\hd"

sc = SparkContext("local", "first app")

# read input text file to RDD
lines = sc.textFile(r"C:\Users\olcay\PycharmProjects\ApacheSpark1\data2.csv")

# map lines to n_words
n_words = lines.distinct().map(lambda line: line.split(","))

llist = n_words.collect()

for i in llist:
    print(i)