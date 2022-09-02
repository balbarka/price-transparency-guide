# Databricks notebook source
# MAGIC %sh
# MAGIC ls -la ../schemas/allowed-amounts/allowed-amounts.json

# COMMAND ----------

import json

with open('../schemas/allowed-amounts/allowed-amounts.json') as fp:
    schema = json.load(fp)

# COMMAND ----------

import json

with open('allowed-amounts/allowed-amounts-single-plan-sample.json') as fp:
    data = '[' + fp.read() + ']'

# COMMAND ----------

df2 = sqlContext.read.json([], schema)
df2.printSchema()

# COMMAND ----------

# DBTITLE 1,Original Schema
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, ArrayType
data2 = []

schema = StructType([ \
    StructField("reporting_entity_name",StringType(),False), \
    StructField("reporting_entity_type",StringType(),False), \
    StructField("plan_name",StringType(),True), \
    StructField("plan_id_type", StringType(), True), \
    StructField("plan_id", LongType(), True), \
    StructField("plan_market_type", StringType(), True), \
    StructField("out_of_network", ArrayType(StructType([
        StructField("name", StringType(), False), \
        StructField("billing_code_type", StringType(), False), \
        StructField("billing_code", StringType(), False), \
        StructField("billing_code_type_version", StringType(), False), \
        StructField("description", StringType(), False), \
        StructField("allowed_amounts", ArrayType(StructType([
            StructField("tin", StructType([
                StructField("type", StringType(), False), \
                StructField("value", StringType(), False)]), False), \
            StructField("service_code", StringType(), True), \
            StructField("billing_class", StringType(), False), \
            StructField("payments", ArrayType(StructType([
                StructField("allowed_amount", FloatType(), False), \
                StructField("billing_code_modifier", StringType(), True), \
                StructField("providers", ArrayType(StructType([
                    StructField("billed_charge", StringType(), False), \
                    StructField("npi", StringType(), False)])), False)]), False), False) ]), False), False) ]), False), False), \
    StructField("last_updated_on", StringType(), False), \
    StructField("version", StringType(), False), \
  ])

df = spark.createDataFrame(data=data2,schema=schema)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Nested Schema
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, ArrayType, FloatType
data2 = []

schema = StructType([ \
    StructField("reporting_entity_name",StringType(),False), \
    StructField("reporting_entity_type",StringType(),False), \
    StructField("plan_name",StringType(),False), \
    StructField("plan_id_type", StringType(), False), \
    StructField("plan_id", LongType(), False), \
    StructField("plan_market_type", StringType(), False), \
    StructField("out_of_network", StructType([
        StructField("name", StringType(), False), \
        StructField("billing_code_type", StringType(), False), \
        StructField("billing_code", StringType(), False), \
        StructField("billing_code_type_version", StringType(), False), \
        StructField("description", StringType(), False)]), False), \
    StructField("allowed_amount", StructType([
        StructField("tin", StructType([
            StructField("type", StringType(), False), \
            StructField("value", StringType(), False)]), False), \
        StructField("service_code", StringType(), False), \
        StructField("billing_class", StringType(), False)])), \
    StructField("payment", StructType([
        StructField("allowed_amount", FloatType(), False), \
        StructField("billing_code_modifier", StringType(), False)]), False),
    StructField("provider", StructType([
        StructField("billed_charge", StringType(), False), \
        StructField("npi", StringType(), False)]), False), \
    StructField("last_updated_on", StringType(), False), \
    StructField("version", StringType(), False), \
  ])

df = spark.createDataFrame(data=data2,schema=schema)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Flat Schema
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, ArrayType, FloatType
data2 = []

schema = StructType([ \
    StructField("reporting_entity_name",StringType(),False), \
    StructField("reporting_entity_type",StringType(),False), \
    StructField("plan_name",StringType(),False), \
    StructField("plan_id_type", StringType(), False), \
    StructField("plan_id", LongType(), False), \
    StructField("plan_market_type", StringType(), False), \
    StructField("out_of_network_name", StringType(), False), \
    StructField("out_of_network_billing_code_type", StringType(), False), \
    StructField("out_of_network_billing_code", StringType(), False), \
    StructField("out_of_network_billing_code_type_version", StringType(), False), \
    StructField("out_of_network_description", StringType(), False), \
    StructField("allowed_amount_tin_type", StringType(), False), \
    StructField("allowed_amount_tin_value", StringType(), False), \
    StructField("allowed_amount_service_code", StringType(), False), \
    StructField("allowed_amount_billing_class", StringType(), False), \
    StructField("payment_allowed_amount", FloatType(), False), \
    StructField("payment_billing_code_modifier", StringType(), False), \
    StructField("provider_billed_charge", StringType(), False), \
    StructField("provider_npi", StringType(), False), \
    StructField("last_updated_on", StringType(), False), \
    StructField("version", StringType(), False) 
    ])

df = spark.createDataFrame(data=data2,schema=schema)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

import pandas as pd
pdf = pd.read_json(data)
=spark.createDataFrame(pdf) 

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import explode, col
display(dat.select("reporting_entity_name", explode(col("out_of_network"))))

# COMMAND ----------

[{tin={type=ein,
       value=1234567890},
       service_code=[01, 02, 03],
       payments=[{allowed_amount=25.0,
                  billing_code_modifier=[25],
                  providers=[{billed_charge=50.0,
                              npi=[1234567891, 1234567892, 1234567893]},
                             {billed_charge=60.0,
                              npi=[1111111111]},
                             {billed_charge=70.0,
                              npi=[2222222222, 3333333333, 4444444444, 5555555555]}]}],
                              billing_class=professional}]
