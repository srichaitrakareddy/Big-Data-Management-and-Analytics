from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *

#Mutual Friends function
def mf(a):
    output = []
    for f in a[1].split(","):
        if len(f) > 0:
            if f > a[0]:
                temp = a[0] + "," + f
            else:
                temp = f + "," + a[0]
            output.append((temp, set(a[1].split(","))))
    return output

#Getting the user details
def user_details(x):
    user = x.split(",")
    return user[0], user[1], user[2], user[3]


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Q2") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    sc = spark.sparkContext
    input_rdd = sc.textFile("file:///Users/srichaitrakareddy/Downloads/soc-LiveJournal1Adj.txt").map(lambda x: x.split("\t"))
    new_rdd = input_rdd.flatMap(mf).reduceByKey(lambda v1, v2: v1.intersection(v2))
    result1_rdd = new_rdd.filter(lambda x: len(x[1]) > 0)\
        .map(lambda x: (x[0].split(",")[0], x[0].split(",")[1],list(x[1])))
    rdd1_df = spark.createDataFrame(result1_rdd)
    count_friends = udf(lambda x: len(x), IntegerType())
    df_top_ten = rdd1_df.select("_1", "_2", count_friends("_3").alias("_3")).sort("_3", ascending=False).head(10)
    top_ten_string = "uid1 uid2 count"
    schema_top_ten = StructType([StructField(each, StringType(), True) for each in top_ten_string.split()])
    df_top10 = spark.createDataFrame(sc.parallelize(df_top_ten), schema_top_ten)
    user_rdd = sc.textFile("file:///Users/srichaitrakareddy/Downloads//userdata.txt").map(user_details)
    user_string = "uid fname lname address"
    schema_user = StructType([StructField(each, StringType(), True) for each in user_string.split()])
    df_user = spark.createDataFrame(user_rdd, schema_user)
    result_first = df_top10.join(df_user, df_user["uid"]==df_top10["uid1"], "cross")
    first_half = result_first.selectExpr("count as count", "uid as uid", "fname as fname1", "lname as lname1",
                                  "address as address1","uid2 as uid2")
    result_second = first_half.join(df_user, df_user["uid"]==first_half["uid2"],"cross")
    new_result = result_second.select(first_half["count"], first_half["fname1"], first_half["lname1"],
                                      first_half["address1"], df_user["fname"], df_user["lname"], df_user["address"])
    new_result.repartition(1).write.save('file:///Users/srichaitrakareddy/HW2_Q2', 'csv', 'overwrite')