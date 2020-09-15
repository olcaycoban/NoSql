from pyspark import SparkContext
import os

os.environ['HADOOP_HOME'] = "C:\\hd"

sc = SparkContext("local", "first app")

# read input text file to RDD
lines = sc.textFile(r"C:\Users\olcay\PycharmProjects\ApacheSpark1\data.csv")

# map lines to n_words
n_words = lines.map(lambda line: line.split(","))

# collect the RDD to a list
llist = n_words.collect()

#print
for word in llist:
    print(word)