'''
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("file:///SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
results = minTemps.collect();

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
'''


from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf= conf)

def parseLine(line):
    fields = line.split(",")
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

def printLine(line):
    print(line)

lines = sc.textFile("file:///C:/ExD\work/big_data/spark/SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)

# Get the min and max temps items only -> result is RDD
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])

# Filter the entryType field as no longed needed
stationTempsMin = minTemps.map(lambda x: (x[0], x[2]))
stationTempsMax = maxTemps.map(lambda x: (x[0], x[2]))
# print each element of stationTemps
# stationTemps.foreach(printLine)

# Aggegrate the RDD to get minimum and maximum temps based on Key (stationID) -->>Result is RDD
minTemps = stationTempsMin.reduceByKey(lambda x, y: min(x, y))
maxTemps = stationTempsMax.reduceByKey(lambda x, y: max(x, y))

# Apply the action on rdd -->>Collect the items from min and max RDDs
resultsMin = minTemps.collect()
resultMax = maxTemps.collect()

print("*** Min Temperature for each station in 1800 ******")
for resultmin in resultsMin:
    print (resultmin[0] + "\t{:.2f}F".format(resultmin[1]))

print("*** Max Temperature for each station in 1800 ******")
for resultmax in resultMax:
    print (resultmax[0] + "\t{:.2f}F".format(resultmax[1]))