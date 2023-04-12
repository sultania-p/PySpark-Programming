from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.sql import functions as func

# create sparksession
spark = SparkSession.builder.appName("PopularMovies").master("local[*]").getOrCreate()

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

# printSchema
movieDF.printSchema()

# get count of ratings/views by movie
countViewsByMovie = movieDF.groupBy("movieID").agg(func.count("movieID").alias("totalViews"))
# sort movies by top views
topMovies = countViewsByMovie.select("*").orderBy("totalViews", ascending= False)

# show top 10 movies data
topMovies.show(10)

# close session
spark.stop()