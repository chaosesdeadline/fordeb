from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, sum
from pyspark.sql.window import Window
from graphframes import GraphFrame

spark = SparkSession.builder.appName("FindUserWithNthChain").getOrCreate()

n = 5

df = spark.read.csv("/home/akaloev/Downloads/tweets/ira_tweets_csv_hashed.csv", header=True)
df.createOrReplaceTempView("tweets")
df = df.withColumn("prev_user_screen_name", lag("user_screen_name", 1).over(Window.orderBy("tweetid")))
df = df.withColumn("is_same_user", col("user_screen_name") == col("prev_user_screen_name"))
df = df.withColumn("group", sum(col("is_same_user").cast("int")).over(Window.orderBy("tweetid")))
result = df.groupBy("user_screen_name", "group").count().orderBy(col("count").desc()).filter(col("count") >= n).limit(1)

result.show()

edges = df.selectExpr("user_screen_name as src", "prev_user_screen_name as dst")
vertices = df.selectExpr("user_screen_name as id").distinct()

g = GraphFrame(vertices, edges)

g.vertices.show()
g.edges.show()

spark.stop()
