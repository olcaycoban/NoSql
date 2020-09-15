from pyspark import SparkContext

logFile = r"C:\Users\olcay\PycharmProjects\ApacheSpark1\deneme.txt"
sc = SparkContext("local", "first app")
logData = sc.textFile(logFile).cache()

numOlcays = logData.filter(lambda s: 'olcay' in s).count()
numCafers = logData.filter(lambda s: 'cafer' in s).count()

firs_data=logData.first()

print("Lines with a: %i, lines with b: %i" % (numOlcays, numCafers))
print("Firs Data is {0}".format(firs_data))