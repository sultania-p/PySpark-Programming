from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql import functions as func

# create sparksession
spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

# create schema for the dataframe (as data is unstructured -->> no headers)
schema = StructType([ \
                        StructField("stationID", StringType(), True),   \
                        StructField("date", IntegerType(), True),   \
                        StructField("measure_type", StringType(), True),    \
                        StructField("temperature", FloatType(), True)])

# create initial dataframe
df = spark.read.schema(schema= schema).csv("file:///C:/ExD\work/big_data/spark/SparkCourse/1800.csv")
df.printSchema()

# filter only TMIN entries
minTemps = df.filter(df.measure_type == "TMIN")
# select only stationId and Temp fields
stationTemps = minTemps.select("stationID", "temperature")
# group on stationID and get the min temperature for each stations
minTempsByStation = stationTemps.groupBy("stationID").agg(func.min("temperature").alias("minTemp"))

# show the data
minTempsByStation.limit(10).show()

# convert the temperature to Fahrenheit
minTempsByStationF = minTempsByStation.withColumn("minTemp",
                                                  func.round((func.col("minTemp") * 0.1 * (9.0 / 5.0) + 32 ), 2)) \
                                                  .select("stationID", "minTemp").sort("minTemp")
minTempsByStationF.show()

# format the temperatures and store in file storage
results = minTempsByStationF.collect()

# print the result
for result in results:
    print (result[0] + "\t{:.2f}F".format(result[1]))

# close connection
spark.stop()