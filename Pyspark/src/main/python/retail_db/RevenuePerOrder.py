
from pyspark import SparkConf, SparkContext

class RevenuePerOrder:
    def main(self):
        conf = SparkConf.setMaster("local").setAppName("Get revenue per Order")
        sc = SparkContext(conf)

        order_items = sc.textFile("/mnt/d/data/retail_db/order_items")
        revenuePerOrder = order_items.map(lambda x:(int(x.split(",")[1]), float(x.split(",")[4]))).reduceByKey(lambda x, y:x+y)
        revenuePerOrder.saveAsTextFile("/mnt/d/data/RevenuePerOrder/python")



