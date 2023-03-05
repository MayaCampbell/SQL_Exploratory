import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import*

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType

import pandas as pd

spark = SparkSession.builder.master("local[*]").appName("Test SQL app").getOrCreate()

import os
username= os.environ.get('USER')
my_password=os.environ.get('PASSWORD')

df = spark.read.json("capstone_files\cdw_sapp_custmer.json")
df2= spark.read.json("capstone_files\cdw_sapp_credit.json")
df3 = spark.read.json("capstone_files\cdw_sapp_branch.json")

df.write.format("jdbc") \
  .mode("append") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_db") \
  .option("dbtable", "cdw_sapp_custmer") \
  .option("user", username) \
  .option("password", my_password) \
  .save()

df2.write.format("jdbc") \
  .mode("append") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_db") \
  .option("dbtable", "cdw_sapp_credit") \
  .option("user", username) \
  .option("password", my_password) \
  .save()

df3.write.format("jdbc") \
  .mode("append") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_db") \
  .option("dbtable", "cdw_sapp_branch") \
  .option("user", username) \
  .option("password", my_password) \
  .save()