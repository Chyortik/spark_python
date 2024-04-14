# Домашнее задание к семинару 3. 
# Условие: используйте источник rate, напишите код, 
# который создаст дополнительный столбец, который будет выводить сумму только нечетных чисел

# Здесь с помощью стрима вводится последовательность чисел от 0 до бесконечности каждую секунду.
# При вводе числа в каждом батче выводится сумма нечетных чисел, введенных до этого
# Операция относится к statefull операциям.

# Импорт необходимых модулей
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Создание сессии SparkSession
spark = SparkSession.builder \
    .master("local") \
    .getOrCreate()

# Создание потока данных (стрима)
rate_stream = spark \
    .readStream \
    .format("rate") \
    .load()

# Операция для подсчета суммы нечетных чисел
odd_rate_sum = rate_stream \
    .groupBy() \
    .agg(F.sum(F.when(F.col("value") % 2 != 0, 
                      F.col("value")).otherwise('next')) \
    .alias("odd_value_sum"))

# Вывести поток данных
query = odd_rate_sum \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Подождать, пока обработка потока данных не будет завершена
query.awaitTermination()