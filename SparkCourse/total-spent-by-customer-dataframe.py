from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from pyspark.sql import functions as func

# cretae sparksession
spark = SparkSession.builder.appName("TotalCustomerExpense").master("local[*]").getOrCreate()

# create schema explicit
schema = StructType([ \
                        StructField("customerID", IntegerType(), True),  \
                        StructField("itemID", IntegerType(), True), \
                        StructField("amountSpent", FloatType(), True)])

df = spark.read.schema(schema= schema).csv("file:///C:/ExD/work/big_data/spark/SparkCourse/customer-orders.csv")
# print schema
df.printSchema()

# select customerID and amountSpent fields
customerSpent = df.select("customerID", "amountSpent")
# total spent by customer
totalspentByCustomer = customerSpent.groupBy("customerID")  \
                                    .agg(func.sum(customerSpent.amountSpent).alias("totalSpent"))
# round amount spent to 2 decimal and then sort desc
totalspentByCustomerR = totalspentByCustomer.select("customerID", func.round(totalspentByCustomer.totalSpent, 2)
                                                        .alias("totalSpent"))   \
                                                        .sort("totalSpent", ascending = False)

# show data
totalspentByCustomerR.show()

# logout session
spark.stop()