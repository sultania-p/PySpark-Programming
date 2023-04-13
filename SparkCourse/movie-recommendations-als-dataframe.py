# Implementing Alternative Least Square model to recommend movie based on ratings
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql import functions as func
import codecs
from pyspark.ml.recommendation import ALS
import sys

# create function to create dict with movieid and title from the u.item
def loadMovieNames():
    movieNames = {}
    with codecs.open("C:/ExD/work/big_data/spark/SparkCourse/ml-100k/u.item", \
                     "r", encoding="ISO-8859-1", errors="ignore") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
        return movieNames

# create sparksession
spark = SparkSession.builder.appName("ALSExample").getOrCreate()

# create schema
movieSchema = StructType([   \
                        StructField("userID", IntegerType(), True), \
                        StructField("movieID", IntegerType(), True), \
                        StructField("rating", IntegerType(), True), \
                        StructField("timestamp", LongType(), True)])

# create dataframe with movie data
ratings = spark.read.option("sep", "\t")    \
                    .schema(schema= movieSchema) \
                    .csv("file:///C:/ExD/work/big_data/spark/SparkCourse/ml-100k/u.data")

# store the names of movies in master node memory for lookup
names = loadMovieNames()

print("*** Training ALS recommendation model")

als = ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userID").setItemCol("movieID").setRatingCol("rating")
model = als.fit(ratings)

# Crete a dataframe for all the userID which we want the recommendation for??
userID = int(sys.argv[1])   # get userID as input
userSchema = StructType([StructField("userID", IntegerType(), True)])   # create schema for userID dataframe
usersDF = spark.createDataFrame([[userID,]], userSchema)

# show dataframe
# ratings.show(10)
# usersDF.show(10)

# get recommendations
recommendations = model.recommendForUserSubset(usersDF, 10).collect()

print("*** Top 10 recommendations for user ID: " + str(userID))

# recommendation returns -> (userID, [Row(movieID, rating), Row(movieID, rating) ...])
for userRecs in recommendations:
    myRecs = userRecs[1]    # -> [Row(movieID, rating), Row(movieID, rating) ...]
    for rec in myRecs:      # -> Row(movieID, rating) ..
        movieID = rec[0]      # -> each movieID for each row element in list
        rating = rec[1]     # -> each rating for each row element in list
        movieName = names[movieID]
        print(movieName + "\t" + str(rating))


# logout session
spark.stop()
