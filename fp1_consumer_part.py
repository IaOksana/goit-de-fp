from itertools import groupby

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr, broadcast, avg, to_json, struct, from_json, current_timestamp
from pyspark.sql.types import StructField, StringType, StructType, IntegerType, LongType
from configs import sql_config, kafka_config
#from kafka import KafkaProducer, KafkaConsumer
#from kafka.admin import KafkaAdminClient, NewTopic
import json
import uuid
import time
import random
import os
from pyspark import SparkContext
#_____________________________________________________
# Пакет, необхідний для читання Kafka зі Spark
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.13:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 pyspark-shell'

# Створення SparkSession

# # Створення Spark сесії
spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "localhost") \
    .config("spark.jars", "/Users/oksana/Workspace/Data-Engineering/goit_de_fp/mysql-connector-j-8.0.32.jar") \
    .appName("Consumer_spark") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Зчитати дані фізичних показників атлетів за допомогою Spark з MySQL таблиці olympic_dataset.athlete_bio
# # Налаштування конфігурації SQL бази даних

# Читання даних з SQL бази даних
df = spark.read.format('jdbc').options(
    url=sql_config['jdbc_url'],
    driver='com.mysql.cj.jdbc.Driver',  # com.mysql.jdbc.Driver
    dbtable=sql_config['bio_table'],
    user=sql_config['jdbc_user'],
    password=sql_config['jdbc_password']).load()

print("athlete_bio successfully loaded")
#df.printSchema()

# 2. Відфільтрувати дані, де показники зросту та ваги є порожніми або не є числами. Можна це зробити на будь-якому етапі вашої програми.
print("start filtering")
df.printSchema()
df_filtered = df \
    .withColumn("height_num", expr("try_cast(height as double)")) \
    .withColumn("weight_num", expr("try_cast(weight as double)")) \
    .filter(col("height_num").isNotNull() & col("weight_num").isNotNull()) \
    .drop("height_num", "weight_num")

print("athlete_bio successfully filtered")
#df_filtered.show(5)
#----------------------------------------------------------

# 3c Зчитати дані з результатами змагань з Kafka-топіку athlete_event_results.
print(f"start downloading from kafka topic")

schema = StructType([
    StructField('edition_id', IntegerType(), True),
    StructField('country_id', StringType(), True),
    StructField('sport', StringType(), True),
    StructField('event', StringType(), True),
    StructField('result_id', LongType(), True),
    StructField('athlete', StringType(), True),
    StructField('athlete_id', IntegerType(), True),
    StructField("pos", StringType(), True),
    StructField("medal", StringType(), True),
    StructField('isTeamSport', StringType(), True)
])

consumer = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .option("subscribe", kafka_config['results_topic_name']) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 1000) \
    .load()

parsed = (
    consumer
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
)
#    .selectExpr("CAST(value AS STRING) AS json_str")
print(f"downloading from kafka topic")


# 4. Об’єднати дані з результатами змагань з Kafka-топіку з біологічними даними з MySQL таблиці за допомогою ключа athlete_id.
print("start joining")
df_joined = parsed.join(broadcast(df_filtered), df_filtered['athlete_id'] == df_filtered['athlete_id'], "left")

# 5. Знайти середній зріст і вагу атлетів індивідуально для кожного виду спорту,
# типу медалі або її відсутності, статі, країни (country_noc).
# Додайте також timestamp, коли розрахунки були зроблені.
print("Знайти середній зріст і вагу атлетів")
df_tmp = (df_joined
            .select(col("medal"), col("sex"), col("country_noc"), col("sport"), col("height"), col("weight")) \
            .filter(col("sex").isNotNull() & col("country_noc").isNotNull() & col("sport").isNotNull()) \
            .groupBy("sport", "medal", "sex", "country_noc") \
            .agg(avg(col("height")).alias("avg_height"), avg(col("weight")).alias("avg_weight")) \
            .withColumn("timestamp", current_timestamp())
)

# 6. Зробіть стрим даних (за допомогою функції forEachBatch) у:
# а) вихідний кафка-топік,
# b) базу даних.

# Функція для обробки кожної партії даних
# У цьому прикладі кожний мікробатч даних записується в MySQL базу даних і Kafka-топік,
# тобто зчитування йде з одного джерела, а запис — у два. Цей патерн називають FanOut.
def foreach_batch_function(batch_df, batch_id):
    # Відправка даних до Kafka
    kafka_df = batch_df.withColumn("batch_id", lit(str(batch_id)))
    kafka_df = (
        kafka_df
        .select(
            to_json(struct(*kafka_df.columns)).alias("value")
        )
    )

    kafka_df \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", ",".join(kafka_config["bootstrap_servers"])) \
        .option("kafka.security.protocol", kafka_config["security_protocol"]) \
        .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
        .option(
            "kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule '
            f'required username="{kafka_config["username"]}" password="{kafka_config["password"]}";'
        ) \
        .option("topic", kafka_config['output_topic_name']) \
        .save()

    # Збереження збагачених даних до MySQL
    batch_df.write \
        .format("jdbc") \
        .option("url", sql_config['jdbc_url']) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", sql_config['output_table']) \
        .option("user", sql_config['jdbc_user']) \
        .option("password", sql_config['jdbc_password']) \
        .mode("append") \
        .save()

df_tmp \
    .writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/checkpoints/oksana_fp1") \
    .start() \
    .awaitTermination()
'''

# --------------------------------------------------------
# 6. forEachBatch: join + агрегации + fan-out в Kafka и в MySQL

def foreach_batch(batch_df, batch_id: int):
    print(f"\n=== BATCH {batch_id} START ===")
    if batch_df.rdd.isEmpty():
        print("Empty batch, skipping.")
        return

    # 4. join streaming событий со статичным bio по athlete_id
    joined = (
        batch_df
        .join(broadcast(df_filtered), on="athlete_id", how="inner")
    )

    print("Joined batch rows:", joined.count())

    # 5. агрегаты: средний рост/вес по sport, medal, sex, country_noc + timestamp
    stats = (
        joined
        .groupBy("sport", "medal", "sex", "country_noc")
        .agg(
            avg(col("height")).alias("avg_height"),
            avg(col("weight")).alias("avg_weight"),
        )
        .withColumn("calculated_at", time.time())
    )

    print("Stats rows:", stats.count())

    # ---------- 6a. запись агрегатов в выходной Kafka-топик ----------
    stats_for_kafka = (
        stats
        .select(to_json(struct(*stats.columns)).alias("value"))
    )

    (
        stats_for_kafka
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
        .option("kafka.security.protocol", "SASL_PLAINTEXT")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option(
            "kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";'
        )
        .option("topic", athlete_event_results_topic_name)
        .save()
    )

    # ---------- 6b. запись агрегатов в MySQL ----------
    (
        stats.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", "athlete_stats_streaming")  # можешь создать такую таблицу
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .mode("append")
        .save()
    )

    print(f"=== BATCH {batch_id} DONE ===")


# Запуск стрима
query = (
    parsed
    .writeStream
    .foreachBatch(foreach_batch)
    .outputMode("append")   # тут мы работаем с микробатчами как с batch DF
    .start()
)

query.awaitTermination()

# 4. Об’єднати дані з результатами змагань з Kafka-топіку з біологічними даними з MySQL таблиці за допомогою ключа athlete_id.
print("start joining")
df_joined = df_filtered.join(broadcast(parsed), parsed['athlete_id'] == df_filtered['athlete_id'], "inner")
df_joined.printSchema()
df_joined.show()


# 5. Знайти середній зріст і вагу атлетів індивідуально для кожного виду спорту,
# типу медалі або її відсутності, статі, країни (country_noc).
# Додайте також timestamp, коли розрахунки були зроблені.

df_tmp = (df_joined.select("medal", "sex", "country_noc", "sport"))

unique_dict = {
    c: [row[c] for row in df_tmp.select(c).distinct().collect()]
    for c in df_tmp.columns
}

columns = []
for col_name, values in unique_dict.items():
    for v in values:
        clean_v = str(v).replace(" ", "_")     # убрать пробелы/символы
        columns.append(f"{col_name}_{clean_v}")

data = [1] * len(columns)
df_final = spark.createDataFrame([data], schema=columns)
df_final.show(truncate=False)
df_final.printSchema()


df_tmp1 = (df_joined.select("weight", "height", "medal", "sex", "country_noc", "sport"))
#        .agg(avg(col("height")).alias("avg_height"), avg(col("weight")).alias("avg_weight"))
 #       .withColumn("calculated_at", time.time())
'''
'''
# 6. Зробіть стрим даних (за допомогою функції forEachBatch) у:
# а) вихідний кафка-топік,
# b) базу даних.

# Функція для обробки кожної партії даних
# У цьому прикладі кожний мікробатч даних записується в MySQL базу даних і Kafka-топік,
# тобто зчитування йде з одного джерела, а запис — у два. Цей патерн називають FanOut.
def foreach_batch_function(batch_df, batch_id):
    kafka_df = batch_df.withColumn('batch_id', batch_id) # TO CHANGE
    # Відправка збагачених даних до Kafka
    kafka_df \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config.bootstrap_servers) \
        .option("topic", output_topic) \
        .save()

    # Збереження збагачених даних до MySQL
    batch_df.write \
        .format("jdbc") \
        .option("url", sql_config['sql_url']) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", jdbc_table) \
        .option("user", jdbc_user) \
        .option("password", jdbc_password) \
        .mode("append") \
        .save()

# Налаштування потоку даних для обробки кожної партії за допомогою вказаної функції
event_stream_enriched = df
event_stream_enriched \
    .writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("update") \
    .start() \
    .awaitTermination()
'''


#empty_batches = 0

from pyspark.sql.streaming import StreamingQueryManager
# def process(batch_df, batch_id):
#     global empty_batches
#
#     if batch_df.count() == 0:
#         empty_batches += 1
#     else:
#         empty_batches = 0
#
#     print(f"[{batch_id}] rows={batch_df.count()}, empty={empty_batches}")
#
#     # если 3 батча подряд пустые — считаем конец
#     if empty_batches >= 3:
#         print("No new messages → stopping stream")
#         query.stop()  # остановить стрим

# query = (
#     parsed.writeStream
#         .foreachBatch(process)
#         .outputMode("append")       # or "update"/"complete" depending on your logic
#         .format("console")          # or "kafka", "parquet", "foreachBatch", etc.
#         .start()
# )
#
# query.awaitTermination()

# query = (
#     parsed.writeStream
#     .format("console")
#     .outputMode("append")
#     .option("truncate", "false")
#     .start()
# )