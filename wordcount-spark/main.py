"""SimpleApp.py"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

print(sys.argv)
if len(sys.argv) != 2:
    sys.exit(2)

logFile = sys.argv[1]
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
textFile = spark.read.text(logFile)

wordCounts = textFile.select(explode(split(textFile.value, "\s+")).alias(
    "word")).groupBy("word").count().sort("count", ascending=False)

wordCounts.write.format('csv').mode('overwrite').saveAsTable('word_freq')

spark.stop()
