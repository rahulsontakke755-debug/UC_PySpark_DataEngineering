# Databricks notebook source
from pyspark.sql import functions as f

base_path = "/Volumes/newcatalog/newschema/newvolume2/UdayanCare_PySpark_Project/source/"
incoming_path = "/Volumes/newcatalog/newschema/newvolume2/UdayanCare_PySpark_Project/incoming/"

# Source data
centres_df = spark.read.csv(base_path + "centres.csv",header=True,inferSchema=True)
programs_df = spark.read.csv(base_path + "programs.csv",header=True,inferSchema=True)
beneficiaries_df = spark.read.csv(base_path + "beneficiaries.csv",header=True,inferSchema=True)
attendance_df = spark.read.csv(base_path + "program_attendance.csv",header=True,inferSchema=True).dropDuplicates()
activities_df = spark.read.csv(base_path + "program_activities.csv",header=True,inferSchema=True)
# Incoming data
new_beneficiaries_df = spark.read.csv(incoming_path + "new_beneficiaries.csv",header=True,inferSchema=True)
new_attendance_df = spark.read.csv(incoming_path + "new_attendance.csv",header=True,inferSchema=True)
new_activities_df = spark.read.csv(incoming_path + "new_activities.csv",header=True,inferSchema=True)
# Existing + new
all_beneficiaries = beneficiaries_df.unionByName(new_beneficiaries_df)
all_attendance = attendance_df.unionByName(new_attendance_df)
all_activities = activities_df.unionByName(new_activities_df)
# Centre information
centres_final = centres_df.select("centre_id","centre_name","city","centre_type")

# Beneficiary KPI
centre_beneficiary = (all_beneficiaries.groupBy("centre_id").count().withColumnRenamed("count", "beneficiary_count"))
# Program KPI
centre_program = (programs_df.groupBy("centre_id").count().withColumnRenamed("count", "program_count"))

# Attendance KPI
attendance_centre = (all_attendance.join(all_beneficiaries,all_attendance.beneficiary_id == all_beneficiaries.beneficiary_id,"inner")
    .select(all_attendance.attendance_id,all_attendance.attendance_status,all_beneficiaries.centre_id)
    .groupBy("centre_id")
    .agg(f.count("*").alias("total_classes"),f.sum(f.when(f.col("attendance_status") == "Present", 1).otherwise(0)).alias("present_classes"))
    .withColumn("attendance_rate",f.round((f.col("present_classes") / f.col("total_classes")) * 100,2)))

# Activity KPI
activity_centre = (all_activities.join(programs_df.select("program_id", "centre_id"),"program_id","inner")
    .groupBy("centre_id").agg(f.count("activity_id").alias("total_activities"),f.sum("participation_count").alias("total_participants")))

# Final output
final_centre_performance = (
    centres_final
    .join(centre_beneficiary, "centre_id", "left")
    .join(centre_program, "centre_id", "left")
    .join(attendance_centre, "centre_id", "left")
    .join(activity_centre, "centre_id", "left"))

display(final_centre_performance)