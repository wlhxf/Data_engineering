from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession,functions

sc = SparkContext('local', 'taobao')
sc.setLogLevel('WARN')
spark = SparkSession.builder.getOrCreate()

df = spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('taobao.csv')
df.count()
df.printSchema()

split_col = functions.split(df['time'], ' ')
df = df.withColumn('date', split_col.getItem(0))
df = df.withColumn('hour', split_col.getItem(1))
df = df.drop("time", "user_geohash")
df.show()

df = df.sort(['date', 'hour', 'user_id', 'item_id'], ascending=True)
df.show()
print(df.count())
#df.write.csv('taobao_clean.csv', header=True)