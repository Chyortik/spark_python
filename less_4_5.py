from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


# Создаем сессию Spark
spark = SparkSession.builder.appName("RateSourceExample").getOrCreate()
# Создаем поток данных с помощью источника "rate"
streamingDF = spark.readStream.format("rate").load()
from pyspark.sql.functions import col
# Добавляем новый столбец "key"
streamingDF = streamingDF.withColumn("key", col("value"))
# Группируем данные по ключу и считаем их количество
groupedData = streamingDF.groupBy("key").count()
# Начинаем обработку потока данных
query = groupedData.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()