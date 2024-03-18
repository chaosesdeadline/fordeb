from pyspark.sql import SparkSession
from graphframes import GraphFrame

# Создаем сессию Spark
spark = SparkSession.builder.appName("IRA Tweets Analysis").getOrCreate()

# Загружаем данные из файла ira_tweets.csv
df = spark.read.csv("ira_tweets.csv", header=True)

# Создаем временное представление для выполнения SQL запросов
df.createOrReplaceTempView("tweets")

# Находим пользователя, начавшего n-ю по количеству сообщений непрерывную цепочку
n = 5  # Укажите значение n

query = f"""
SELECT user_screen_name, COUNT(*) AS message_count
FROM (
    SELECT user_screen_name, ROW_NUMBER() OVER (ORDER BY tweet_time) - ROW_NUMBER() OVER (PARTITION BY user_screen_name ORDER BY tweet_time) AS grp
    FROM tweets
)
GROUP BY user_screen_name, grp
HAVING COUNT(*) >= {n}
ORDER BY message_count DESC
LIMIT 1
"""

result = spark.sql(query)

# Выводим результат
result.show()

# Закрываем сессию Spark
spark.stop()
