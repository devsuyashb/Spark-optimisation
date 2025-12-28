# Databricks notebook source
# MAGIC %md
# MAGIC used to add who create a table and all details

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime

# COMMAND ----------

import uuid

# COMMAND ----------

# adding audit info to raw tables
def add_audit_info(df, run_id):
    """
    Adds audit information columns to a Pyspark DataFrame.

    Parameters:
        df(Pyspark.sql.DataFrame): The DataFrame to which the audit information will be added,
        run_id(str): The run identifier to be added as an audit column.

    Returns:
        pyspark.sql.DataFrame: The DataFrame with added audit information columns.

    """

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    username = spark.sql("select current_user() as username").collect()[0][0]

    df = df.withColumn("run_id", lit(run_id))\
        .withColumn("timestamp", unix_timestamp(lit(timestamp), 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))\
        .withColumn("user", lit(username))\
        .withColumn("is_active", lit('Y'))

    return df


# COMMAND ----------

from pyspark.sql.functions import rand, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("salary", DoubleType(), True)
])

names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Helen"]
cities = ["New York", "San Francisco", "London", "Berlin", "Tokyo", "Sydney", "Paris", "Toronto"]

df = spark.createDataFrame(
    [(names[i % len(names)],
      int(20 + (i * 7) % 30),
      cities[i % len(cities)],
      float(40000 + (i * 1234) % 60000))
     for i in range(100)],
    schema
).withColumn("salary", expr("salary + rand() * 5000"))

display(df)

# COMMAND ----------

# write it inot schema

run_id= str(uuid.uuid4())
df = add_audit_info(df, run_id)
df.write.mode("overwrite").saveAsTable("maincode.test.Table1_audit_ex")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from maincode.test.Table1_audit_ex