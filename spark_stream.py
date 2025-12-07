import logging
from datetime import datetime

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, count, avg
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, from_json, to_timestamp, window, count, sha2

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streaming
        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """)
    
    logger.info("Keyspace created successfully")

def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streaming.aggregated_counts (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            users_registered_count INT,
            PRIMARY KEY (window_start, window_end) # 复合主键，确保唯一性
        );
        """)
    
    logger.info("Aggregated Table created successfully")


def insert_data(session, **kwargs):
    logger.info("Inserting data")

    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streaming.created_users (first_name, last_name, gender, address,
                        post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        logger.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logger.error(f"Error while inserting data: {e}")



def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .config('spark.cassandra.connection.host', 'cassandra_db') \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
        logger.info("Spark connection created successfully")

    except Exception as e:
        logger.error(f"Error while creating spark connection: {e}")
    
    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logger.info("Kafka dataframe created successfully")
    except Exception as e:
        logger.error(f"Kafka dataframe could not be created because: {e}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error details: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
    
    if spark_df is None:
        logger.error("Failed to create Kafka dataframe")
    
    return spark_df

def create_cassandra_connection():
    try:
        # Connection to Cassandra cluster
        cluster = Cluster(['cassandra_db'])
        cas_session = cluster.connect()
        logger.info("Cassandra connection created successfully")
        return cas_session
    
    except Exception as e:
        logger.error(f"Error while creating Cassandra connection: {e}")
        return None

def create_selection_df_from_kafka(spark_df, timestamp_col="registered_date"):
    schema = StructType([
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField(timestamp_col, StringType(), True),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)

    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    processed_df = sel.withColumn(
        "email_hash", sha2(col("email"), 256)

   timestamp_df = processed_df.withColumn(
        "event_time", 
        to_timestamp(col("registered_date"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") 
    )

    watermarked_df = timestamp_df.withWatermark("event_time", "1 minute")

    aggregated_df = watermarked_df \
        .groupBy(
            window(col("event_time"), "1 minute") # 1分钟滚动窗口
        ) \
        .agg(
            count("*").alias("users_registered_count"), # 聚合：计算窗口内用户数量
            # 可以根据需要添加其他聚合，例如 avg(col("age"))
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("users_registered_count")
        )

    logger.info("Aggregated dataframe created successfully")
    return aggregated_df

if __name__ == "__main__":
    # Create Spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # Create connection to Kafka with Spark
        spark_df = connect_to_kafka(spark_conn)
        aggregated_df = create_selection_df_from_kafka(spark_df)

        logger.info("Aggregated dataframe schema:")
        aggregated_df.printSchema()

        # Create Cassandra connection
        session = create_cassandra_connection()
        
        if session is not None:
            create_keyspace(session)
            create_table(session)

            # Insert data into Cassandra
          
            streaming_query = selection_df.writeStream.format("org.apache.spark.sql.cassandra") \
                    .option('keyspace', 'spark_streaming', ) \
                    .option('checkpointLocation', '/tmp/checkpoint') \
                    .option('table', 'aggregated_counts') # 指向新的聚合表
                    .outputMode('update') # 使用 update 模式，适合聚合的持续输出
                    .start()

            streaming_query.awaitTermination()
            session.shutdown()
