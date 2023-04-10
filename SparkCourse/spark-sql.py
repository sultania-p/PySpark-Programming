# we create sparksession in spark sql, ROW is each dataset record
from pyspark.sql import SparkSession, Row
# create a SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# create mapper function to structure as rows with each values from RDD
# constructing row objects such as attributes
def mapper(line):
    fields = line.split(",")
    return Row(ID= int(fields[0]), name= str(fields[1].encode("utf-8")), \
               age= int(fields[2]), numFriends= int(fields[3])) 


# create an RDD (lines) for the unstructured forrmat (as header does not exists)
lines = spark.sparkContext.textFile("file:///C:/ExD/work/big_data/spark/SparkCourse/fakefriends.csv")

# Map each input from RDD to a structure row in transformed RDD
#  => Return RDD with datasets as Rows to be converted into DataFrames 
people = lines.map(mapper)

# Infer the schema from RDD and create/register the Dataframe as a table
schemaPeople = spark.createDataFrame(people).cache()

# In order to query the data from dataframe like database table, create a temporary view
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that are registeredas a table
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

# Result of SQL queries are RDD and support all normal RDD operaions
for teen in teenagers.collect():
    print(teen)

# We can also use SQL functions on DataFrame instead of queries
schemaPeople.groupBy("age").count().orderBy("age").show()

# remember to stop the session like DB instance
spark.stop()