'''from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read each line of my book into a dataframe
inputDF = spark.read.text("file:///SparkCourse/book.txt")

# Split using a regular expression that extracts words
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
wordsWithoutEmptyString = words.filter(words.word != "")

# Normalize everything to lowercase
lowercaseWords = wordsWithoutEmptyString.select(func.lower(wordsWithoutEmptyString.word).alias("word"))

# Count up the occurrences of each word
wordCounts = lowercaseWords.groupBy("word").count()

# Sort by counts
wordCountsSorted = wordCounts.sort("count")

# Show the results.
wordCountsSorted.show(wordCountsSorted.count())
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# reading each line into a dataframe
inputDF = spark.read.text("file:///C:/ExD/work/big_data/spark/SparkCourse/book.txt")
inputDF.printSchema()

# Select words using regex
words = inputDF.select(func.split(inputDF.value, "\\W+").alias("value"))
# Split each row (list) of dataframe list into rows in dataframe
wordsSplit = words.select(func.explode(words.value).alias("word"))
# Filter any empty rows
wordsWithoutEmptyString = wordsSplit.filter(wordsSplit.word != "")
# Normalize the words
wordsLowerCased = wordsWithoutEmptyString.select(func.lower(wordsWithoutEmptyString.word)   \
                                                 .alias("word"))
# Count of word occurences
countWords = wordsLowerCased.groupBy("word").agg(func.count("word").alias("wordCount"))
# sorted count words
countWordsSorted = countWords.sort("wordCount", ascending= False)





# print to see data in dataframe
countWordsSorted.limit(20).show()

# close session
spark.stop()