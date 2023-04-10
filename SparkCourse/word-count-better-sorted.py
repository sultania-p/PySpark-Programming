'''import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///sparkcourse/book.txt")
words = input.flatMap(normalizeWords)

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
'''


from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf= conf)

# print elemtns of RDD
def print_element(line):
    print(line)

# handle special characters using regular expression
def normalizeWords(line):
    return re.compile(r'\W+', re.UNICODE).split(line.lower())

# create the RDD by loading the text data
lines = sc.textFile("file:///C:/ExD/work/big_data/spark/SparkCourse/Book.txt")

# split each values in RDD as seperate values in transformed RDD
# handle special cahracters <help> is same as <help.>
word = lines.flatMap(normalizeWords)   # retuns an RDD

# Get count by each value as key value [(this, 1), [is, 1) ..]
wordCounts = word.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()

results = wordCountsSorted.collect()

# convert any unicide characters to ascii and supress errors, iterate through list
for result in results:
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print (word.decode() + "\t" + ":\t" + str(result[0]))
