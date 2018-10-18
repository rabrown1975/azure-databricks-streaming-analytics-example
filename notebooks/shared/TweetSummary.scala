// Databricks notebook source
val t2 = spark.read.parquet("/work/output/tweets")
display(t2)

// COMMAND ----------


