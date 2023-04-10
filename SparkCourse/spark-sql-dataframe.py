from pyspark.sql import SparkSession

# create SparkSession for Spark SQL
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# As here in the source file, the shcema is defined (headers) so no need to convert into RDD and back to DF
# If schema is fixed we can directly sue spark.read option
people = spark.read.option("header", "true").option("inferSchema", "true") \
            .csv("file:///C:/ExD/work/big_data/spark/SparkCourse/fakefriends-header.csv")


print("*** Here is the infered schema ****")
people.printSchema()

print("**** Display name column ****")
people.select("name").show()

print("**** Filter persons over age 21 ****")
people.filter(people.age > 21).show()

print("**** Total people by age group ****")
people.groupBy("age").count().show()

print("**** Raise each age by 10 years ****")
people.select(people.name, people.age + 10).show()



spark.stop()