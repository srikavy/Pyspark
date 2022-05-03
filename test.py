import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.Builder.appName('Dataframe').getOrCreate()
spark
df_pyspark = spark.read.csv('test.csv')
df_pyspark.show()


