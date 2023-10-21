import sys
import configparser
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from google.cloud import bigquery
from google.cloud.exceptions import exceptions
from google.api_core.exceptions import AlreadyExists

def interact_google_pubsub_subscription():

    """
        This function perform connection to Google BigQuery.
        Also create Dataset and Target table in Google BigQuery.
    """
    
    global dataset, table

    # Initialize the BigQuery client
    try:
        client = bigquery.Client(project=project_id)
    except Exception as e:
        print(f"Error cannot create connection with BigQuery client: {e}")
        sys.exit(1)

    # Create dataset
    try:
        dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
        dataset = client.create_dataset(dataset, exists_ok=True)
        print(f"Dataset '{dataset}' has been created.")
    except AlreadyExists:
        print(f"Dataset '{dataset}' already exists.")
    except Exception as e:
        print(f"Error cannot create DataSet in BigQuery {e}")
        sys.exit(1)

    # Define table schema
    schema = [
        bigquery.SchemaField('event_id', 'STRING'),
        bigquery.SchemaField('name', 'STRING'),
        bigquery.SchemaField('event_name', 'STRING'),
        bigquery.SchemaField('category', 'STRING'),
        bigquery.SchemaField('item_id', 'STRING'),
        bigquery.SchemaField('item_quantity', 'INT64'),
        bigquery.SchemaField('event_time', 'TIMESTAMP'),
    ]

    # Create table in BigQuery
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        table = client.get_table(table_ref)
        print(f"Table '{table_id}' already exists.")
    except exceptions.NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f"Table '{table_id}' has been created.")
    except Exception as e:
        print(f"Error creating BigQuery table: {e}")
        sys.exit(1)

# Define a function to read message and load into Google BigQuery
def process_notification(messages):

    if not messages.isEmpty():

        # Define the schema for the incoming JSON messages
        message_schema = StructType([
            StructField('event_id', StringType(), True),
            StructField('name', StringType(), True),
            StructField('event_name', StringType(), True),
            StructField('category', StringType(), True),
            StructField('item_id', StringType(), True),
            StructField('item_quantity', IntegerType(), True),
            StructField('event_time', StringType(), True),
        ])

        # Parse the JSON messages
        message_df = messages.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), message_schema).alias("message")).select("message.*")

        # Perform necessary data transformations here if needed
        processed_data = message_df.withColumn("event_name", col("event_name").replace("_", " ").alias("event_name")).withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss").alias("event_time"))

        # Write the processed data to BigQuery
        processed_data.write \
            .format('bigquery') \
            .option('table', f'{project_id}:{dataset}.{table_id}') \
            .mode('append') \
            .save()

def main():
    interact_google_pubsub_subscription()

    try:
        print("================================================")
        print("============ Start Spark Streaming ============")
        print("================================================")
        print()
        print("Streaming is available ...")
        print()

        spark = SparkSession.builder.appName("Spark2BigQuery").getOrCreate()
        
        # Define a Structured Streaming DataFrame from Kafka source
        kafkaStream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_broker) \
            .option("subscribe", kafka_topic) \
            .load()

        messages = kafkaStream.selectExpr("CAST(value AS STRING)")

        query = messages.writeStream \
            .foreach(process_notification) \
            .start()

        query.awaitTermination()
    except KeyboardInterrupt as e:
        print("================================================")
        print("============ Stop Spark Streaming =============")
        print("================================================")
        print()
        print("Streaming is unavailable ...")


######################################
############ MAIN PROGRAM ############
######################################

if __name__ == '__main__':

    # Initialize the configuration parser
    config = configparser.ConfigParser()

    # Read configuration file
    config.read("./config.ini")

    try:
        kafka_broker = config.get('PROJ_CONF', 'KAFKA_BROKER')
        kafka_topic = config.get('PROJ_CONF', 'KAFKA_TOPIC')
        project_id = config.get('PROJ_CONF', 'PROJ_ID')
        dataset_id = config.get('PROJ_CONF', 'DATASET_NAME')
        table_id = config.get('PROJ_CONF', 'TABLE_NAME')
    except Exception as e:
        print(f"Error cannot get require parameters: {e}")
        sys.exit(1)

    # Initialize Spark Session
    spark = SparkSession.builder.appName("Spark2BigQuery").getOrCreate()
    
    main()