from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName('WordCount')
sc = SparkContext(conf=conf)

rdd = sc.textFile("C:/ExD/work/big_data/spark/SparkCourse/ml-100k/uz.txt")

words = rdd.flatMap(lambda line: line.split(" "))
# print(words.collect())
print('total number of words is: ' + str(words.count()))

wordCount = words.map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y)
# print(wordCount.collect())
