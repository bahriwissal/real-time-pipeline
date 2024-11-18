import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthenticator
from cassandra.cluster import Cluster

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType




def create_keyspace(session):
    session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : '1'};
    """)
    
    print("Keyspace created ")

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

    print("Table created ")


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
        logging.error(f"data couldn't be inserted due to {e}")


def create_spark_connection():
    try : 
        spark_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        
        spark_conn.sparkContext.setLogLevel('ERROR')
        logging.info('Spark connection created')

        return spark_conn
    
    except Exception as e :
        logging.error(f"Spark session couldn't be created due to {e}")

        return None
    
def connect_to_kafka(spark_conn):
    try:
        # Use spark connection to read data from kafka 
        print(spark_conn)
        spark_df = spark_conn.readStream.format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'latest') \
            .load()
        logging.info("kafka dataframe created")

        return spark_df

    except Exception as e:
        logging.error(f"kafka dataframe couldn't be created due to: {e}")
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

def create_cassandra_connection():
    try : 
        # Connecting to cassandra cluster 
        cluster = Cluster(['localhost'])
        cassandra_session = cluster.connect()
        return cassandra_session 
    except Exception as e : 
        logging.error(f"Cassadra session couldn't be created due to {e}")

        return None

if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        print("conneting to kafka ")
        spark_df = connect_to_kafka(spark_conn)
        print(spark_df)
        selection_df = create_selection_df_from_kafka(spark_df)
        cassandra_session = create_cassandra_connection()

        if cassandra_session is not None:
            create_keyspace(cassandra_session)
            create_table(cassandra_session)

            # logging.info("Streaming is being started...")

            # streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
            #                    .option('keyspace', 'spark_streams')
            #                    .option('table', 'created_users')
            #                    .start())

            # streaming_query.awaitTermination()