from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, udf,count, when
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
import avro.schema
import fastavro
from fastavro.schema import load_schema
import fastavro.schema
import avro.io
import io

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("AdvertiseX Data Pipeline") \
    .getOrCreate()

# Define schema for JSON data (Ad Impressions)
ad_impressions_schema = StructType([
    StructField("ad_creative_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("website", StringType(), True)
])

# Define schema for CSV data (Clicks and Conversions)
clicks_conversions_schema = StructType([
    StructField("event_timestamp", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("ad_campaign_id", StringType(), True),
    StructField("conversion_type", StringType(), True)
])

# Define schema for fastavro data (Bid Requests)
bid_requests_schema = fastavro.schema.Parse("""
{
  "type": "record",
  "name": "BidRequest",
  "fields": [
    {"name": "user_id", "type": "string"},
    {"name": "auction_id", "type": "string"},
    {"name": "ad_targeting_criteria", "type": "string"},
    {"name": "timestamp", "type": "string"}
  ]
}
""")


# Data validation, filtering, and deduplication
def validate_and_filter(df, required_columns):
    # Check for nulls
    for column in required_columns:
        df = df.filter((col(column).isNotNull()) & (col(column) != ""))
    
    # Remove duplicates
    df = df.dropDuplicates()
    
    return df
    

# Data bid_requests_df avro data reading through pandas to spark
def bid_requests_df_func(bid_requests_data_file):
    with open(bid_requests_data_file, 'rb') as f:
        bid_requests_reader = fastavro.reader(f)
        bid_requests_data = [record for record in bid_requests_reader]
    bid_requests_df = pd.DataFrame(bid_requests_data)
    return spark.createDataFrame(bid_requests_df)
    

# Read JSON ad impressions data
ad_impressions_df = spark.read.schema(ad_impressions_schema).json("ad_impressions_data.json")
ad_impressions_df=validate_and_filter(ad_impressions_df, ['user_id','timestamp'])

# Read CSV clicks and conversions data
clicks_conversions_df = spark.read.schema(clicks_conversions_schema).csv("clicks_conversions_data.csv")
clicks_conversions_df=validate_and_filter(clicks_conversions_df, ['user_id','event_timestamp'])

# Function to parse Avro data
def parse_avro(record):
    reader = avro.io.DatumReader(bid_requests_schema)
    bytes_reader = io.BytesIO(record)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    return reader.read(decoder)


# Read Avro bid requests data
bid_requests_rdd = spark.sparkContext.binaryFiles("bid_requests_data.avro") \
    .flatMap(lambda x: [parse_avro(record) for record in x[1]])

# Convert RDD to DataFrame
bid_requests_df = spark.createDataFrame(bid_requests_rdd)

'''
# Read Avro bid requests data through pandas to spark
bid_requests_df = bid_requests_df_func('bid_requests_data_data.avro')
'''

bid_requests_df = validate_and_filter(bid_requests_df, ['user_id','timestamp'])


# Convert timestamps to appropriate format
ad_impressions_df = ad_impressions_df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))
clicks_conversions_df = clicks_conversions_df.withColumn("event_timestamp", to_timestamp(col("event_timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))
bid_requests_df = bid_requests_df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))

# Join dataframes to correlate ad impressions with clicks and conversions
correlated_df = ad_impressions_df.join(clicks_conversions_df, on="user_id", how="inner") \
    .join(bid_requests_df, on="user_id", how="inner")


# Write the processed data to a suitable storage format
correlated_df.write.mode("overwrite").parquet("processed_data.parquet")

# Create a table for analytical queries
correlated_df.createOrReplaceTempView("campaign_performance")

# Example analytical query
query = """
SELECT ad_campaign_id, COUNT(*) as total_interactions
FROM campaign_performance
GROUP BY ad_campaign_id
ORDER BY total_interactions DESC
"""
spark.sql(query).show()



# Detect anomalies or discrepancies
anomalies_df = correlated_df.withColumn("anomaly", when(col("ad_creative_id").isNull(), 1).otherwise(0))
anomaly_count = anomalies_df.filter(col("anomaly") == 1).count()


# Implement alerting mechanism
if anomaly_count > 0:
    print(f"Alert: {anomaly_count} anomalies detected in the data pipeline")

# Example of data quality check
data_quality_df = correlated_df.groupBy("ad_campaign_id").agg(count("*").alias("interaction_count"))
data_quality_df.show()

# Save data quality report
data_quality_df.write.mode("overwrite").csv("data_quality_report.csv")
