from pyspark.sql import SparkSession
from pyspark import SparkConf

# Spark Configuration
conf = SparkConf()
conf.setAppName("read dataframe")

# Initialize Spark Session
spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

# Dictionary with export paths for different environments
path_export_data = {
    'databricks': '/dbfs/mnt/landing_zone/real_time_customer_csv/*.csv',
    'fabric': 'Files/*.csv',
    'docker': 's3a://landing-zone/customer_real_time/*.csv'
}

# Define the path to the CSV file
csv_file_path = path_export_data['fabric']

# Read the CSV file into a DataFrame
df = spark.read.format("csv").option("header", "true").load(csv_file_path)

# Show the first few rows of the DataFrame
df.show()
