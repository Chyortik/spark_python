# стрим stateless операции:
# Импортируем необходимые модули
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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
# Применяем stateless операцию к потоку данных
# Мы умножаем каждое значение на 2
result = streaming_df.select(col("value") * 2)
# Выводим результат
query = result \
.writeStream \
.outputMode("append") \
.format("console") \
.start()
query.awaitTermination()