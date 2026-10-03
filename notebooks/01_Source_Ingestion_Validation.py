# Databricks notebook source
from pyspark.sql import functions as f

base_path = "/Volumes/newcatalog/newschema/newvolume2/UdayanCare_PySpark_Project/source/"

centres_df = spark.read.csv(
    base_path + "centres.csv",
    header=True,
    inferSchema=True
)

programs_df = spark.read.csv(
    base_path + "programs.csv",
    header=True,
    inferSchema=True
)

beneficiaries_df = spark.read.csv(
    base_path + "beneficiaries.csv",
    header=True,
    inferSchema=True
)

attendance_df = spark.read.csv(
    base_path + "program_attendance.csv",
    header=True,
    inferSchema=True
)

activities_df = spark.read.csv(
    base_path + "program_activities.csv",
    header=True,
    inferSchema=True
)

print("Source files loaded successfully")

# COMMAND ----------

print("Centres:", centres_df.count())
print("Programs:", programs_df.count())
print("Beneficiaries:", beneficiaries_df.count())
print("Attendance:", attendance_df.count())
print("Activities:", activities_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### NULL validation:

# COMMAND ----------

print("Centres NULL:")
centres_df.select([
    f.count(f.when(f.col(c).isNull(), c)).alias(c)
    for c in centres_df.columns
]).show()

print("Programs NULL:")
programs_df.select([
    f.count(f.when(f.col(c).isNull(), c)).alias(c)
    for c in programs_df.columns
]).show()

print("Beneficiaries NULL:")
beneficiaries_df.select([
    f.count(f.when(f.col(c).isNull(), c)).alias(c)
    for c in beneficiaries_df.columns
]).show()

print("Attendance NULL:")
attendance_df.select([
    f.count(f.when(f.col(c).isNull(), c)).alias(c)
    for c in attendance_df.columns
]).show()

print("Activities NULL:")
activities_df.select([
    f.count(f.when(f.col(c).isNull(), c)).alias(c)
    for c in activities_df.columns
]).show()
