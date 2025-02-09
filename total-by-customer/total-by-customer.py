from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName('TotalByCustomer').getOrCreate()

schema = StructType([\
                    StructField('customerId', IntegerType(), True),\
                    StructField('itemId', IntegerType(), True),\
                    StructField('price', FloatType(), True),\
                    ])

purchases = spark.read.schema(schema).csv('customer-orders.csv')
total_by_customer = purchases.select('customerId', 'price')\
                            .groupBy('customerId')\
                            .agg(
                                func.round(
                                    func.sum('price'),
                                    2
                                ).alias('total')
                            )

sorted_total_by_customer = total_by_customer.sort('total')
sorted_total_by_customer.show(sorted_total_by_customer.count())

spark.stop()

