# Ваша задача:
# Припустимо, ви працюєте в букмекерській конторі. Одне з ваших завдань — генерація даних для ML-моделей. Деякі з features — це середній зріст, середня вага, стать, країна походження атлетів.
# Моделі тренуються окремо для кожного виду спорту. Дані про наявність медалі у виступі використовуються як target (output) для ML-моделі.
# Фірма хоче коригувати коефіцієнти ставок якомога швидше, тому вас попросили розробити відповідне рішення з використанням стримінгу.
# Фізичні дані атлетів відомі заздалегідь і зберігаються в MySQL базі даних. Результати змагань же подаються через Kafka-топік.
#
#
# Ваша задача:
# 1. Зчитати дані фізичних показників атлетів за допомогою Spark з MySQL таблиці olympic_dataset.athlete_bio (база даних і Credentials до неї вам будуть надані).
#
# 2. Відфільтрувати дані, де показники зросту та ваги є порожніми або не є числами. Можна це зробити на будь-якому етапі вашої програми.
#
# 3. Зчитати дані з mysql таблиці athlete_event_results і записати в кафка топік athlete_event_results. Зчитати дані з результатами змагань з Kafka-топіку athlete_event_results. Дані з json-формату необхідно перевести в dataframe-формат, де кожне поле json є окремою колонкою.
#
# 4. Об’єднати дані з результатами змагань з Kafka-топіку з біологічними даними з MySQL таблиці за допомогою ключа athlete_id.
#
# 5. Знайти середній зріст і вагу атлетів індивідуально для кожного виду спорту, типу медалі або її відсутності, статі, країни (country_noc). Додайте також timestamp, коли розрахунки були зроблені.

# 6. Зробіть стрим даних (за допомогою функції forEachBatch) у:
# а) вихідний кафка-топік,
# b) базу даних.

from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

#_____________________________________________________
# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# 1. Створення топіків в Kafka:
output_topic = NewTopic(name=kafka_config['output_topic_name'], num_partitions=kafka_config['num_partitions'], replication_factor=kafka_config['replication_factor'])
results_topic = NewTopic(name=kafka_config['results_topic_name'], num_partitions=kafka_config['num_partitions'], replication_factor=kafka_config['replication_factor'])

# Створення нового топіку
try:
    admin_client.create_topics(new_topics=[output_topic], validate_only=False)
    print(f"Topic '{kafka_config['output_topic_name']}' created successfully.")
except Exception as e:
    print(f"An error occurred: {e} with topic '{kafka_config['output_topic_name']}'.")

try:
    admin_client.create_topics(new_topics=[results_topic], validate_only=False)
    print(f"Topic '{kafka_config['results_topic_name']}' created successfully.")
except Exception as e:
    print(f"An error occurred: {e} with topic '{kafka_config['results_topic_name']}'.")


# Перевіряємо список існуючих топіків
print(admin_client.list_topics())

# Закриття зв'язку з клієнтом
admin_client.close()
