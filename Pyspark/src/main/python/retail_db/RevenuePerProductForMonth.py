import sys
import configparser as cp

try:
    from pyspark import SparkContext, SparkConf

    props = cp.RawConfigParser()
    props.read("../../Resources/application.ini")
    # env = sys.argv[1]
    conf1 = SparkConf().setMaster(props.get(sys.argv[5], 'executionMode')).setAppName("Revenue Per Month")
    sc = SparkContext(conf=conf1)
# input dir, output base dir, local base dir, month, environment
    inputPath = sys.argv[1]
    outPath = sys.argv[2]
    month = sys.argv[3]

    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Configuration = sc._gateway.jvm.org.apache.hadoop.fs.Configuration
    fs = FileSystem.get((Configuration()))

    if(fs.exists(Path(inputPath)) == False):
        print("Input path does not exists")
    else:
        if(fs.exists(Path(outPath))):
            fs.delete(Path(outPath), True)

        orders = inputPath + "/orders"
        ordersFiltered = sc.textFile(orders).\
            filter(lambda order:month in order.split(",")[1]).\
            map(lambda order: (int(order.split(",")[0]), 1))

        orderItems = inputPath + "/order_items"
        revenueByProductId = sc.textFile(orderItems). \
            map(lambda orderItem:
                (int(orderItem.split(",")[1]),
                 (int(orderItem.split(",")[2]), float(orderItem.split(",")[4])
                  ))
                ). \
            join(ordersFiltered). \
            map(lambda rec: rec[1][0]). \
            reduceByKey(lambda total, ele: total + ele)
        localPath = sys.argv[4]
        productsFile = open(localPath + "/products/part-00000")
        products = productsFile.read().splitlines()

        # Convert into RDD and extract product_id and product_name
        # Join it with aggregated order_items (product_id, revenue)
        # Get product_name and revenue for each product
        sc.parallelize(products). \
            map(lambda product:
                (int(product.split(",")[0]), product.split(",")[2])). \
            join(revenueByProductId). \
            map(lambda rec: rec[1][0] + "\t" + str(rec[1][1])). \
            saveAsTextFile(outPath)

        print("Successfully imported Spark Modules")

except ImportError as e:
    print("can not import spark modules", e)

sys.exit(1)


