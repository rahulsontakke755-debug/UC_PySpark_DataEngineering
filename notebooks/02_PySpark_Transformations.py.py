# Databricks notebook source
from pyspark.sql import functions as f
from pyspark.sql.window import Window

base_path = "/Volumes/newcatalog/newschema/newvolume2/UdayanCare_PySpark_Project/source/"

centres_df = spark.read.csv( base_path + "centres.csv",header=True,inferSchema=True)

programs_df = spark.read.csv(base_path + "programs.csv",header=True,inferSchema=True)

beneficiaries_df = spark.read.csv(base_path + "beneficiaries.csv",header=True,inferSchema=True)

attendance_df = spark.read.csv(base_path + "program_attendance.csv",header=True,inferSchema=True)

activities_df = spark.read.csv(base_path + "program_activities.csv",header=True,inferSchema=True)

print("Source data loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Cleaning

# COMMAND ----------


attendance_clean = attendance_df.dropDuplicates()

centres_df = centres_df.withColumn("city",f.upper(f.col("city")))

print("Cleaning completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning completed

# COMMAND ----------

program_centre = centres_df.join(programs_df,centres_df.centre_id == programs_df.centre_id,"inner").select(
    centres_df.centre_id,
    centres_df.centre_name,
    centres_df.city,
    centres_df.centre_type,
    programs_df.program_id,
    programs_df.program_name,
    programs_df.start_date,
    programs_df.end_date)

display(program_centre)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Beneficiaries + Centres join

# COMMAND ----------

beneficiary_centre = beneficiaries_df.join(centres_df,"centre_id","inner")

display(beneficiary_centre)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Attendance + Beneficiary join

# COMMAND ----------

attendance_beneficiary = attendance_clean.join(beneficiary_centre,attendance_clean.beneficiary_id == beneficiary_centre.beneficiary_id,"inner").select(
    attendance_clean.attendance_id,
    attendance_clean.beneficiary_id,
    attendance_clean.program_id,
    attendance_clean.attendance_date,
    attendance_clean.attendance_status,
    beneficiary_centre.centre_id,
    beneficiary_centre.centre_name,
    beneficiary_centre.city,
    beneficiary_centre.age_group,
    beneficiary_centre.gender,
    beneficiary_centre.enrollment_date)

display(attendance_beneficiary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Activities + Program join

# COMMAND ----------

Activities_Program = activities_df.join(program_centre,activities_df.program_id == program_centre.program_id,"inner").select(
    activities_df.activity_id,
    activities_df.program_id,
    activities_df.activity_date,
    activities_df.activity_type,
    activities_df.participation_count,
    program_centre.program_name,
    program_centre.centre_id,
    program_centre.centre_name,
    program_centre.city)

display(Activities_Program)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Activity Aggregation

# COMMAND ----------

activity_summary = (
    Activities_Program
    .groupBy("program_id", "program_name", "centre_id")
    .agg(
        f.count("activity_id").alias("total_activities"),
        f.sum("participation_count").alias("total_participants")
    )
)

display(activity_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Window Function + Top Program per Centre.

# COMMAND ----------

rule = Window.partitionBy("centre_id").orderBy(
    f.col("total_participants").desc()
)

top_program_per_centre = (
    activity_summary
    .withColumn(
        "program_rank",
        f.dense_rank().over(rule)
    )
    .filter(
        f.col("program_rank") == 1
    )
)

display(top_program_per_centre)

# COMMAND ----------

# MAGIC %md
# MAGIC
