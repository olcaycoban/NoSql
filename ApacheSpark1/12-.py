from pyspark import SparkContext
import os

os.environ['HADOOP_HOME'] = "C:\\hd"

sc = SparkContext("local", "first app")

# read input text file to RDD
lines = sc.textFile(r"C:\Users\olcay\PycharmProjects\ApacheSpark1\data2.csv")

# map lines to n_words
n_words = lines.map(lambda line: line.split(","))

print(n_words.take(3))

def f(x):
    print(x)

n_words.foreach(f)