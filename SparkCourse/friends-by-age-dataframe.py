from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as func

# create SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# Data is structured format (with headers), so directly read data
friends = spark.read.option("header", "true")   \
                .option("inderSchema", "true")  \
                .csv("file:///C:/ExD/work/big_data/spark/SparkCourse/fakefriends-header.csv")

# print(type(friends))     -->> DataFrame

# print the schema
print("*** Before column type update ***")
friends.printSchema()

# convert friends (String to Integer type)
friends_new = friends.withColumn("friends", friends["friends"].cast(IntegerType()))
# print("*** After column type update ***")
# friends_new.printSchema()

friendsbyAge = friends_new.select("age", "friends")
print("** Query using Spark SQL functions ***")
friendsbyAge.groupBy("age").avg("friends").sort("age").show()

# Show amountspent in formatted way
friendsbyAge.groupBy("age").agg(func.round(func.avg("friends"), 2).alias("avg_friends")).sort("age").show()

print("*** Using SQL Query ***")
friendsbyAge.createOrReplaceTempView("VW_FriendsByAge")
# spark.sql("SELECT age, AVG(friends) from VW_FriendsByAge GROUP BY age").show()

# stop spark sql instance
spark.stop()