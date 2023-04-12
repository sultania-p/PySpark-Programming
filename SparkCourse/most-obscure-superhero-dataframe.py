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
getConnections = lines.withColumn("id", func.split(func.col("value"), " ")[0])    \
                            .withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1)    \
                            .groupBy("id").agg(func.sum("connections").alias("connections"))
# find minimum connections in data
minConnectionCount = getConnections.agg(func.min("connections")).first()[0]

minConnections = getConnections.filter(func.col("connections") == minConnectionCount)

print("** Below superheroes have only " + str(minConnectionCount) + " connection(s). **")
# get least popular superhero
leastPopularNames = minConnections.join(names, minConnections.id == names.id).select("name")

# show data
leastPopularNames.show()

# logout session
spark.stop()
