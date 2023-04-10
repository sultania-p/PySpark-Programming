from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as func

# create SparkSession
spark = SparkSession.builder.appName("EmpployeeSalary").getOrCreate()

# create the dataframe
employees = spark.read.option("header", "true") \
                        .option("inferSchema", "true")   \
                        .csv("file:///C:/ExD/work/big_data/spark/SparkCourse/salaryData.csv")

employees.printSchema()

deptSalary = employees.select("deptName", "salary")

# Get total salary by department
deptSalary.groupBy("deptName").agg(func.sum("salary").alias("totalSalary")).show()

# Max salary by department
deptSalary.groupBy("deptName").agg(func.max("salary").alias("maxSalary")).show()

# Avg salary by department
deptSalary.groupBy("deptName").agg(func.round(func.avg("salary"),2).alias("avgSalary")).show()

# using Spark SQL query
employees.createOrReplaceTempView("VW_EmpSalary")
dept_salary = spark.sql("select deptName, round(avg(salary), 2) as avgSalary from VW_EmpSalary group by deptName")

for sal in dept_salary.collect():
    print(sal)

# logout session
spark.stop()
