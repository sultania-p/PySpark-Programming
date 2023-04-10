
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalAmountByCustomer")
sc = SparkContext(conf= conf)

# To print elements/values of RDD
def print_element(line):
    print(line)

# Extract the fields customerId and AmountSpent
def parseLine(line):
    fields = line.split(",")
    customerID = int(fields[0])
    amountSpent = float(fields[2])
    return (customerID, amountSpent)

# create the raw RDD
lines = sc.textFile("file:///C:/ExD\work/big_data/spark/SparkCourse/customer-orders.csv")
parsedLines = lines.map(parseLine)

# Aggregate amount spent by custmomerID
totalSpentbyCustomer = parsedLines.reduceByKey(lambda x,y: x + y)

# sort the total spent by descending order
sortedTotalSpent = totalSpentbyCustomer.map(lambda x: (x[1], x[0])).sortByKey(ascending= False)

# collect the value (Action)
results = sortedTotalSpent.collect()

# print the values
for result in results:
    print(str(result[1]) + ":\t" + "{:.2f}".format(result[0]))


# totalSpent.foreach(print_element)