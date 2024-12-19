# Import pyspark Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, when, regexp_replace, trim

# Initialize Spark Session
spark = SparkSession.builder.appName("Cloud computing assignment").getOrCreate()

# Load the dataset
bucket_name = "dcu-cc-bucket"
file_name = "dcu-cc-project.csv"
dataset_path = f"s3://{bucket_name}/{file_name}"
data = spark.read.csv(dataset_path, header=True, inferSchema=True)


df = data

# Drop null rows
df = df.dropna()

# Read the file with the correct delimiter
df = spark.read.option("header", True).option("delimiter", ";").csv(dataset_path)
print(df.columns)

# Viewing values inside "Keywords" column
df.select("Keywords").distinct().show(truncate=False)

# Filter data for students in Portugal
filtered_data = df.filter(col('Student Country') == 'Portugal')

# Calculate success rate for each topic and question level
success_rate = (
    filtered_data.groupBy('Topic', 'Question Level')
    .agg(
        count('Type of Answer').alias('Total Answers'),
        _sum('Type of Answer').alias('Correct Answers')
    )
)

# Add a new column for Success Rate
success_rate = success_rate.withColumn('Success Rate', (col('Correct Answers') / col('Total Answers')) * 100)

# Find topics that have both 'Basic' and 'Advanced' questions
topics_with_both_levels = (
    success_rate.groupBy('Topic')
    .agg(count('Question Level').alias('Level Count'))
    .filter(col('Level Count') > 1)
    .select('Topic')
)

# Filter success_rate to include only those topics and exclude 'Optimization'
finalized_data = (
    success_rate.join(topics_with_both_levels, on='Topic', how='inner')
    .filter(col('Topic') != 'Optimization')
)

# Define the output path for finalized data in S3 bucket "dcu-cc-project"
output_bucket_name = "dcu-cc-bucket"  # Ensure this is the same bucket
output_file_name = "finalized_data.csv"
output_path = f"s3://{output_bucket_name}/{output_file_name}"

# Save the filtered data as a CSV in the S3 bucket
finalized_data.write.option("header", "true").csv(output_path)





