from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql import functions as func
import codecs

# create sparksession
spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# create schema
schema = StructType([   \
                        StructField("userID", IntegerType(), True), \
                        StructField("movieID", IntegerType(), True), \
                        StructField("rating", IntegerType(), True), \
                        StructField("timestamp", LongType(), True)])

# create dataframe with movie data
movieDF = spark.read.option("sep", "\t")    \
                    .schema(schema= schema) \
                    .csv("file:///C:/ExD/work/big_data/spark/SparkCourse/ml-100k/u.data")

# create function to create dict with movieid and title from the u.item
def loadMovieNames():
    movieNames = {}
    with codecs.open("C:/ExD/work/big_data/spark/SparkCourse/ml-100k/u.item", \
                     "r", encoding="ISO-8859-1", errors="ignore") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
        return movieNames
    
# broadcast the u.item data to executors
nameDict = spark.sparkContext.broadcast(loadMovieNames())

# get movie counts
movieCounts = movieDF.groupBy("movieID").count()

# create a UDF to lookup movie based on movieID from our broadcasted dictionary
def lookupMovieName(movieID):
    return nameDict.value[movieID]

lookupMovieNameUDF = func.udf(lookupMovieName)

# Add movieTitle column in movieCounts dataframe
MoviesWithNames = movieCounts.withColumn("movieTitle", lookupMovieNameUDF(func.col("movieID")))

# sort by movie counts
sortedMovieWithNames = MoviesWithNames.orderBy(func.desc("count"))

# show data
sortedMovieWithNames.show(10)

# logout session
spark.stop()



