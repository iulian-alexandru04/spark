from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalByCustomer")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customer = int(fields[0])
    amount = float(fields[2])
    return (customer, amount)

lines = sc.textFile("customer-orders.csv")
purchases = lines.map(parseLine)
total_by_customer = purchases.reduceByKey(lambda x, y: x + y)
sorted_total_by_customer = total_by_customer.sortBy(lambda x: x[1])
restults = sorted_total_by_customer.collect()

for r in restults:
    print(r)

