'''from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///sparkcourse/book.txt")
words = input.flatMap(lambda x: x.split())
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
'''


from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf= conf)

# print elemtns of RDD
def print_element(line):
    print(line)

# create the RDD by loading the text data
lines = sc.textFile("file:///C:/ExD/work/big_data/spark/SparkCourse/Book.txt")
# split each values in RDD as seperate values in transformed RDD using .flatmap()
word = lines.flatMap(lambda x: x.split())   # retuns an RDD
# Get count by each value using countByValue action on RDD
wordCounts = word.countByValue()    # returns a list

# convert any unicide characters to ascii and supress errors, iterate through list
for word, count in wordCounts.items():
    cleanWord = word.encode(encoding= "ascii", errors= "ignore")
    if (cleanWord):
        print (cleanWord.decode(), str(count))

