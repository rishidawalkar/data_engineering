import logging

import pyspark, os, sys
from cassandra.cluster import Cluster
from pyspark.sql import *
from pyspark import SparkConf,SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import types
from pyspark import StorageLevel
from pyspark.sql.functions import broadcast


#keyspace in cassandra is similar to database in RDBMS
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_stream
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)

    print("keyspace created successfully")


def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_stream.created_users (
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
            picture TEXT
        )
    """)

    print("Table created successfully")


#kwargs (keyword-arguments) allow you to pass parameters to the functions which is used to interact with database.
#Here we are passing column values as parameters using get() function and passing it to variables, which would then get inserted into table.
def insert_data(session, **kwargs):
    print("inserting data..")

    user_id = kwargs.get("id")
    first_name = kwargs.get("first_name")
    last_name = kwargs.get("last_name")
    gender = kwargs.get("gender")
    address = kwargs.get("address")
    postcode = kwargs.get("postcode")
    email = kwargs.get("email")
    username = kwargs.get("username")
    dob = kwargs.get("dob")
    registered_date = kwargs.get("registered_date")
    phone = kwargs.get("phone")
    picture = kwargs.get("picture")

    try:
        session.execute("""
            INSERT INTO spark_stream.created_users(user_id, first_name, last_name, gender, address, postcode, email, username, dob, registered_date, phone, picture)
            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (user_id, first_name, last_name, gender, address, postcode, email, username, dob, registered_date, phone, picture))

        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f"couldnt insert data due to {e}")


def create_spark_connection():

    spark=None
    try:
        spark=SparkSession.builder() \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.13:3.41,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1")\
            .config('spark.cassandra.connection.host', 'localhost')\
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")  # you are instructing Spark to only log error messages and suppress logs of other levels such as INFO, DEBUG, and WARN
        logging.info("Spark connection created successfully")

    except Exception as e:
        logging.error(f"couldnt connect to spark due to : {e}")

    return spark

# we have above used "com.datastax.spark:spark-cassandra-connector_2.13:3.41,"
# and "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1" configs to connect spark with cassandra database and kafka


#below we are using spark connection to read data from kafka
def connect_to_kafka(spark):
    spark_df=None
    try:
        spark_df=spark.readStream \
            .format('kafka')\
            .option('kafka.bootstrap.servers','localhost:9092')\
            .option('subscribe', 'users_created')\
            .option('startingOffsets', 'earliest')\
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"Kafka dataframe couldnt be created due to : {e}")

    return spark_df


def create_selection_df_from_kafka(spark_df):
    schema=StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("postcode", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel=spark_df.selectExpr("CAST(value as String)")\
        .select(from_json(col('value'), schema).alias('data')).select('data.*')

    print(sel)

    return sel

def create_cassandra_connection():

    try:
        #connecting to cassandra cluster
        cluster = Cluster(["localhost"])

        cas_session=cluster.connect()
        return cas_session

    except Exception as e:
        logging.error(f"couldnt connect to cassandra cluster due to: {e}")
        return None



if __name__ == "__main__":
    spark_conn=create_spark_connection()

    if spark_conn is not None:
        df=connect_to_kafka(spark_conn)
        selection_df=create_selection_df_from_kafka(df)
        session=create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            #insert_data(session)

        streaming_query= (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                          .option('keyspace', 'spark_stream')
                          .option('table', 'created_users')
                          .start())

        streaming_query.awaitTermination()