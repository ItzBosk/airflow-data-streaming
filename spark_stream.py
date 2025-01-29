import logging
import sys

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):

    print("inserting data...")

    user_id = kwargs.get('id')
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
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')


def create_spark_connection():

    connection = None

    try:
        # use jars packages to create support for connection with kafka and cassandra
        connection = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        connection.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")

    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return connection


def connect_to_kafka(spark_conn):
    
    # streaming dataframe
    spark_df = None
    
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        
        logging.info("kafka dataframe created successfully")

    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster by generating a session
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):

    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    selection = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    
    print(selection)
    return selection


if __name__ == "__main__":
    try:
        # Spark Connection
        spark_conn = create_spark_connection()
        if spark_conn is None:
            logging.error("Unable to create Spark connection")
            sys.exit(1)  # Esce con errore

        # Kafka Connection
        spark_df = connect_to_kafka(spark_conn)
        if spark_df is None:
            logging.error("Unable to connect to Kafka with Spark")
            sys.exit(1)

        selection_df = create_selection_df_from_kafka(spark_df)
        if selection_df is None:
            logging.error("Error creating the selected DataFrame")
            sys.exit(1)

        # Cassandra Connection
        cass_session = create_cassandra_connection()
        if cass_session is None:
            logging.error("Unable to connect to Cassandra")
            sys.exit(1)

        # Crete keyspace and table
        create_keyspace(cass_session)
        create_table(cass_session)

        logging.info("Streaming starting...")

        # Starting streaming query (update results with new arriving messages)
        streaming_query = (
            selection_df.writeStream.format("org.apache.spark.sql.cassandra")
            .option("checkpointLocation", "/tmp/checkpoint")
            .option("keyspace", "spark_streams")
            .option("table", "created_users")
            .start()
        )

        streaming_query.awaitTermination()  # visualize streaming process

    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)  # script terminates with an error code

    finally:
        # Cleanup: Closing connections
        if spark_conn:
            logging.info("Close Spark connection")
            spark_conn.stop()
        if cass_session:
            logging.info("Close Cassandra connection")
            cass_session.shutdown()

