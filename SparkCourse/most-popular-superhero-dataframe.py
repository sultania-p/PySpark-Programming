'''from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:///SparkCourse/Marvel-names.txt")

lines = spark.read.text("file:///SparkCourse/Marvel-graph.txt")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
mostPopular = connections.sort(func.col("connections").desc()).first()

mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")

'''

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as func

# create spark session
spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

# Define the scheme for Marvel-Names dataset
schema = StructType([   \
                        StructField("id", IntegerType(), True), \
                        StructField("name", StringType(), True)
])

# create the dataframe - Marvel-Names and Marvel-Graph
names = spark.read.schema(schema= schema).option("sep", " ")    \
                    .csv("file:///C:/ExD/work/big_data/spark/SparkCourse/Marvel-Names.txt")

lines = spark.read.text("file:///C:/ExD/work/big_data/spark/SparkCourse/Marvel-Graph.txt")

# Create the count of connections for every actor
getConnectionsPerRow = lines.withColumn("id", func.split(func.col("value"), " ")[0])    \
                            .withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1)    \

# Gross count for each superhero id
mostPopular = getConnectionsPerRow.groupBy("id").agg(func.sum("connections").alias("connections"))  \
                                                            .sort(func.col("connections").desc()).first()
# get most popular superhero
mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

# show data
print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances")

# logout session
spark.stop()
