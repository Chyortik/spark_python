# фильтрация данных:
# Импортируем необходимые модули
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
# Создаем SparkSession
spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()
# Создаем поток данных с использованием источника данных `rate`
streaming_df = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
# Применяем stateless операцию фильтрации к потоку данных
# Мы оставляем только те строки, где значение больше 5
result = streaming_df.filter(col("value") > 5)
# Выводим результат
query = result \
.writeStream \
.outputMode("append") \
.format("console") \
.start()
query.awaitTermination()