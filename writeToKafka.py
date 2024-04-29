from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws



def write_to_kafka(csv_path, topic_name, bootstrap_servers):
    

    spark = SparkSession.builder \
        .appName("CSV to Kafka") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")\
        .getOrCreate()

    # Read CSV data as a DataFrame
    df = spark.read \
        .format("csv") \
        .option("header", True)  \
        .option("delimiter", ",")  \
        .load(csv_path)

   
       
        
    string_df = df.select(concat_ws(",",*df.columns).alias("value"))


    string_df.write \
            .format("kafka") \
            .option("topic", topic_name) \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .save()

    spark.stop()


if __name__ == "__main__":
    csv_path = "/opt/spark/spark-app/spark_df/placement.csv"  
    topic_name = "topic1" 
    bootstrap_servers = "localhost:9092"  


    write_to_kafka(csv_path, topic_name, bootstrap_servers)
