from pyspark import SparkContext, SparkConf


def business(a):
    bus = a.split("::")
    return bus[0], bus[2]


def review(a):
    rev = a.split("::")
    return rev[2], (rev[1], rev[3])


if __name__ == "__main__":
    conf = SparkConf().setAppName("Q3").setMaster("local")
    sc = SparkContext(conf=conf)
    business = sc.textFile("file:///Users/srichaitrakareddy/Downloads/business.csv")
    review = sc.textFile("file:///Users/srichaitrakareddy/Downloads/review.csv")
    bus_rdd = business.map(business).filter(lambda a: "Colleges & Universities" in a[1])
    rev_rdd = review.map(review)
    new_rdd = rev_rdd.join(bus_rdd).distinct().map(lambda a: (a[1][0][0], a[1][0][1]))
    res_rdd = new_rdd.map(lambda a: (str(a[0]) + "\t" + str(a[1])))
    res_rdd.repartition(1).saveAsTextFile("file:///Users/srichaitrakareddy/HW2_Q3")