# Импортируем необходимые модули
from pyspark.sql import SparkSession
# Создаем SparkSession
spark = SparkSession.builder \
.appName("StructuredNetworkWordCount") \
.getOrCreate()
# Создаем поток данных с использованием источника данных `rate`
streaming_df = spark \
.readStream \
.format("rate") \
.option("rowsPerSecond", 1) \
.load()
# Выводим поток данных
query = streaming_df \
.writeStream \
.outputMode("append") \
.format("console") \
.start()
query.awaitTermination()