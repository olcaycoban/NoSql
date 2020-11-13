from pyspark import SparkContext
import os

os.environ['HADOOP_HOME'] = "C:\\hd"

sc = SparkContext("local", "first app")

# read input text file to RDD
lines = sc.textFile(r"C:\Users\olcay\PycharmProjects\ApacheSpark1\data2.csv")

# map lines to n_words
n_words = lines.map(lambda line: line.split(","))

# filter lines to n_words
n_words2 = n_words.filter(lambda line: int(line[5]) > 45)


llist = n_words2.collect()

for i in llist:
    print(i)