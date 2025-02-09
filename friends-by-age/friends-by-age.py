from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('FriendsByAge').getOrCreate()

people = spark.read.option('header', 'true').option('inferSchema', 'true')\
        .csv('fakefriends-header.csv')

print('Average friends by age:')
people.select('age', 'friends').groupBy('age').avg('friends').sort('age').show()

spark.stop()

