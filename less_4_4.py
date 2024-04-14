# Импортируем необходимые модули
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
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
# Добавляем уникальный идентификатор к каждой строке
streaming_df = streaming_df.withColumn("uniqueId",expr("monotonically_increasing_id()"))
# Удаляем дубликаты без водяного знака
no_watermark_deduplication = streaming_df.dropDuplicates("uniqueId")
# Удаляем дубликаты с водяным знаком
with_watermark_deduplication = streaming_df \
    .withWatermark("timestamp", "10 seconds") \
    .dropDuplicates("uniqueId")
# Выводим результаты
query1 = no_watermark_deduplication \
.writeStream \
.outputMode("append") \
.format("console") \
.start()
query2 = with_watermark_deduplication \
.writeStream \
.outputMode("append") \
.format("console") \
.start()
query1.awaitTermination()
query2.awaitTermination()