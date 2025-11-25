
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from configs import sql_config, kafka_config
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from pyspark.sql.functions import to_json, struct
import json

#-------------------------------------------------------------------------------------
# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)
# value_serializer=lambda v: json.dumps(v).encode('utf-8'),
# key_serializer=lambda v: json.dumps(v).encode('utf-8')

# # Створення Spark сесії
spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "localhost") \
    .config("spark.jars", "/Users/oksana/Workspace/Data-Engineering/goit_de_fp/mysql-connector-j-8.0.32.jar") \
    .appName("JDBCToKafka") \
    .getOrCreate()

#_------------------------------------------------------------------------------------
# 3. Зчитати дані з mysql таблиці athlete_event_results і
# записати в кафка топік athlete_event_results.
# Зчитати дані з результатами змагань з Kafka-топіку athlete_event_results.
# Дані з json-формату необхідно перевести в dataframe-формат, де кожне поле json є окремою колонкою.

# 3a. Зчитати дані з mysql таблиці athlete_event_results
# Читання даних з SQL бази даних
athlete_event_results_df = spark.read.format('jdbc').options(
    url=sql_config['jdbc_url'],
    driver='com.mysql.cj.jdbc.Driver',  # com.mysql.jdbc.Driver
    dbtable=sql_config['results_table'],
    user=sql_config['jdbc_user'],
    password=sql_config['jdbc_password'],) \
    .load()

print(" ")
print("Athlete_event_results read successfully.")
athlete_event_results_df.printSchema()
athlete_event_results_df.show(5)

# 3b.записати в кафка топік athlete_event_results.
df_for_kafka = athlete_event_results_df.select(
    to_json(struct(*[col(c) for c in athlete_event_results_df.columns])).alias("value")
)

print(df_for_kafka.count())
rows = df_for_kafka.collect()

count_sent = False
i = 0
try:
    for row in rows:
        i = i + 1
        if count_sent == False:
            #producer.send(kafka_config['results_topic_name'], value=str(df_for_kafka.count()).encode("utf-8"))
            count_sent = True
        producer.send(kafka_config['results_topic_name'], value=row["value"].encode("utf-8"))
        if i % 50000 == 0:
            print(f"{i} records sent")
    producer.flush()
    print(f"Sent to {kafka_config['results_topic_name']}")
except Exception as e:
    print(f"An error occurred: {e}")

producer.close()  # Закриття producer
spark.stop()

