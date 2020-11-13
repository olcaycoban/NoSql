from pyspark import SparkContext
import os

os.environ['HADOOP_HOME'] = "C:\\hd"

sc = SparkContext("local", "first app")

# read input text file to RDD
lines = sc.textFile(r"C:\Users\olcay\PycharmProjects\ApacheSpark1\data.csv")

# map lines to n_words
n_words = lines.map(lambda line: line.split(","))

# filter lines to n_words
n_words2 = n_words.filter(lambda line: line[4] == "Russia" and line[3]=="Male")

llist = n_words2.collect()

"""for n in llist:
    print(n)
"""
print(len(llist))

for i in llist:
    print(i[0],"-------",i[4])

""""
# collect the RDD to a list
llist = n_words.collect()

#print
for word in llist:
    print(word)
"""