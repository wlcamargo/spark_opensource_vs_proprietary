from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from pyspark import SparkConf

# Spark Configuration
conf = SparkConf()
conf.setAppName("Dashboard")

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

# Count the number of customers per country
country_counts = df.groupBy("country").count()

# Convert to Pandas for plotting
country_counts_pd = country_counts.toPandas()

# Sort the data by customer count
country_counts_pd = country_counts_pd.sort_values(by="count", ascending=False)

# Create the bar chart
plt.figure(figsize=(10, 6))
bars = plt.bar(country_counts_pd["country"], country_counts_pd["count"], color='blue')
plt.title('Distribution of Customer Counts by Country')
plt.xlabel('Country')
plt.ylabel('Customer Count')
plt.xticks(rotation=45)
plt.grid(axis='y')

# Add labels to the bars
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval, int(yval), va='bottom')

# Show the plot
plt.show()