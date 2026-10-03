# Databricks notebook source
# MAGIC %md
# MAGIC ### new incoming data load + duplicate validation + union

# COMMAND ----------

from pyspark.sql import functions as f

incoming_path = "/Volumes/newcatalog/newschema/newvolume2/UdayanCare_PySpark_Project/incoming/"

new_beneficiaries_df = spark.read.csv(incoming_path + "new_beneficiaries.csv",header=True,inferSchema=True)

new_attendance_df = spark.read.csv(incoming_path + "new_attendance.csv",header=True,inferSchema=True)

new_activities_df = spark.read.csv(incoming_path + "new_activities.csv",header=True,inferSchema=True)

print("Incoming data loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ### duplicate validation + existing data ke saath union

# COMMAND ----------

# Load existing source data
source_path = "/Volumes/newcatalog/newschema/newvolume2/UdayanCare_PySpark_Project/source/"

beneficiaries_df = spark.read.csv(source_path + "beneficiaries.csv",header=True,inferSchema=True)

attendance_df = spark.read.csv(source_path + "program_attendance.csv",header=True,inferSchema=True).dropDuplicates()

activities_df = spark.read.csv(source_path + "program_activities.csv",header=True,inferSchema=True)


all_beneficiaries = beneficiaries_df.unionByName(new_beneficiaries_df)
all_attendance = attendance_df.unionByName(new_attendance_df)
all_activities = activities_df.unionByName(new_activities_df)

print("Duplicate Beneficiaries:")
all_beneficiaries.groupBy("beneficiary_id").count().filter("count > 1").show()

print("Duplicate Attendance:")
all_attendance.groupBy("attendance_id").count().filter("count > 1").show()

print("Duplicate Activities:")
all_activities.groupBy("activity_id").count().filter("count > 1").show()
