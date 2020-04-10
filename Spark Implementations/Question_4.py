from pyspark import SparkContext, SparkConf


def business(a):
    bus = a.split("::")
    return bus[0], (bus[1], bus[2])


def review(a):
    rev = a.split("::")
    return rev[2], rev[3]


if __name__ == "__main__":
    conf = SparkConf().setAppName("Q4").setMaster("local")
    sc = SparkContext(conf=conf)
    business = sc.textFile("file:///Users/srichaitrakareddy/Downloads/business.csv")
    review = sc.textFile("file:///Users/srichaitrakareddy/Downloads/review.csv")
    bus_rdd = business.map(business).filter(lambda x: "NY" in x[1][0])
    rev_rdd = review.map(review)
    ag_rating_rdd = rev_rdd.mapValues(lambda v: (float(v), 1))\
        .reduceByKey(lambda a,b: (a[0] + b[0], a[1] + b[1]))\
        .mapValues(lambda v: v[0] / v[1])\
        .sortBy(lambda a: (a[1], a[0]), ascending=False, numPartitions=1)

    top_rdd = ag_rating_rdd.join(bus_rdd).distinct().top(10, key=lambda x: x[1])
    format_rdd = sc.parallelize(top_rdd)

    res_rdd = format_rdd.map(lambda x: str(x[0]) + "\t" + str(x[1][1][0]) + "\t" + str(x[1][1][1]) + "\t" + str(x[1][0]))
    res_rdd.saveAsTextFile("file:///Users/srichaitrakareddy/HW2_Q4")