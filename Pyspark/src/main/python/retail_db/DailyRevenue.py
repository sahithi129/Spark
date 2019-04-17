
from pyspark import SparkContext, SparkConf
import configparser as cp
import sys

props = cp.RawConfigParser()

# Path to configuration file "application.ini"

props.read("src/main/Resources/application.ini")
env = sys.argv[1]

conf1 = SparkConf().setMaster(props.get(env, 'executionMode')).setAppName("Daily Revenue")
sc = SparkContext(conf=conf1)


orders = sc.textFile(props.get(env, 'input.base.dir')+"/orders")
order_items = sc.textFile(props.get(env, 'input.base.dir')+"/order_items")

ordersFiltered = orders.filter(lambda x:x.split(",")[3] in ("COMPLETE", "CLOSED"))
ordersMap = ordersFiltered.map(lambda o:(int(o.split(",")[0]), o.split(",")[1]))

orderItemsMap = order_items.map(lambda x:(int(x.split(",")[1]), float(x.split(",")[4])))

ordersJoin = ordersMap.join(orderItemsMap)

ordersJoinMap = ordersJoin.map(lambda x: x[1])

dailyRevenue = ordersJoinMap.reduceByKey(lambda x, y:x+y)

dailyRevenueSorted = dailyRevenue.sortByKey()
dailyRevenueSortedMap = dailyRevenueSorted.map(lambda oi: oi[0] + "," + str(oi[1]))
dailyRevenueSortedMap.saveAsTextFile(props.get(env, 'output.base.dir')+"/daily_revenue_app")

