from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
import time

# Spark Configuration
conf = SparkConf()
conf.setAppName("Generate Customer Real Time")

# Initialize Spark session
spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

# Function to generate random data
def generate_random_data():
    id = random.randint(1, 1000)
    firstname = f"First_{id}"
    lastname = f"Last_{id}"
    country = random.choice(["Portugal", "USA", "Angola", "Brazil", "Argentina"])
    return (firstname, lastname, id, country)

# Schema for the DataFrame
schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("country", StringType(), True)
])

# Dictionary with export paths for different environments
path_export_data = {
    'databricks': '/dbfs/mnt/landing_zone/real_time_customer_csv/customer_',
    'fabric': 'Files/customer_',
    'docker': 's3a://landing-zone/customer_real_time/customer_'
}

# Loop to create CSV files with random data every 10 seconds
while True:
    # Generate random data
    data = [generate_random_data() for _ in range(100)]  # Generate 100 rows of data
    
    # Create DataFrame with generated data
    df = spark.createDataFrame(data=data, schema=schema)
    
    # Define export path based on current environment
    export_data = path_export_data.get('fabric')  
    
    # Save DataFrame as CSV file
    timestamp = int(time.time())
    filename = f"{export_data}{timestamp}.csv"
    df.write.format("csv").mode("overwrite").option("header", "true").save(filename)
    
    print('file created successfully!')
    
    # Wait for 10 seconds before generating the next set of data
    time.sleep(10)
    print




