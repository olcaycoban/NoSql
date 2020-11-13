from pyspark import SparkContext
import os

from Person import Person

os.environ['HADOOP_HOME'] = "C:\\hd"

sc = SparkContext("local", "first app")

# read input text file to RDD
lines = sc.textFile(r"C:\Users\olcay\PycharmProjects\ApacheSpark1\data.csv")

# map lines to n_words
n_words = lines.map(lambda line: line.split(","))

# collect the RDD to a list
llist = n_words.collect()

sınıf=[]
#print
for word in llist:
    person=Person()
    person.set_first_name(first_name=word[0])
    person.set_second_name(second_name=word[1])
    person.set_email(email=word[2])
    person.set_gender(gender=word[3])
    person.set_country(country=word[4])
    sınıf.append(word)

for per in sınıf:
    print(per[0],"---",per[1],"---",per[2],"---",per[3],"---",per[4])