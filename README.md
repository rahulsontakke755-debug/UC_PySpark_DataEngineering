# UC_Pyspark–Centre & Program Performance Analytics using PySpark
## Project Overview
An end-to-end PySpark Data Engineering project built using Azure Databricks for a simulated NGO analytics use case.
The project demonstrates how source CSV data can be ingested, validated, transformed, analyzed and processed through a Databricks Workflow.
> Note: The dataset used in this project is synthetic/anonymized and created for learning and portfolio demonstration purposes.

## Technology Stack
- Python
- PySpark
- Azure Databricks
- Databricks Workflows / Jobs
- CSV

## Project Workflow
Source CSV Files
        ↓
Source Ingestion & Validation
        ↓
PySpark Transformations
        ↓
Incremental Processing
        ↓
Final Centre Performance
        ↓
Business KPIs

## Project Modules
### 1. Source Ingestion & Validation
- Read CSV source files using PySpark
- Schema inference
- Record count validation
- NULL validation
- Duplicate validation
- Basic data quality checks

### 2. PySpark Transformations
- Data cleaning
- DataFrame transformations
- Joins
- Aggregations
- Centre and program analysis
- Attendance analysis
- Activity analysis
- Window functions:
  - ROW_NUMBER
  - RANK
  - DENSE_RANK

### 3. Incremental Processing
- Process newly received beneficiary data
- Process newly received attendance data
- Process newly received activity data
- Union existing and incoming datasets
- Validate duplicate business keys after processing

### 4. Final Centre Performance
Final business output includes:
- Centre information
- Beneficiary count
- Program count
- Total classes
- Present classes
- Attendance rate
- Total activities
- Total participants

## Spark Optimization
The project also demonstrates:
- Repartition
- Coalesce
- Cache
- Broadcast Join

## Databricks Workflow
The project is orchestrated using a 4-task Databricks Workflow:
1. Source_Ingestion_Validation
2. PySpark_Transformations
3. Incremental_Processing
4. Final_Centre_Performance
Tasks are configured with dependencies to execute the pipeline sequentially.

## Repository Structure
```text
UC_PySpark_DataEngineering/
│
├── 01_Source_Ingestion_Validation.py
├── 02_PySpark_Transformations.py
├── 03_Incremental_Processing.py
├── 04_Final_Centre_Performance.py
└── README.md
