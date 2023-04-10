'''import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///sparkcourse/book.txt")
words = input.flatMap(normalizeWords)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
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
# Get count by each value using countByValue action on RDD
wordCounts = word.countByValue()    # returns a list

# convert any unicide characters to ascii and supress errors, iterate through list
for word, count in wordCounts.items():
    cleanWord = word.encode(encoding= "ascii", errors= "ignore")
    if (cleanWord):
        print (cleanWord.decode(), str(count))

