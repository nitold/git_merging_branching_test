from pyspark.sql import SparkSession

def init_spark():
    spark = SparkSession.builder.appName("Accumulator and Broadcast Program").master("local").getOrCreate()
    sc = spark.sparkContext
    spark.sparkContext.setLogLevel("ERROR")
    return spark, sc

def main():
    spark, sc = init_spark()
    file_1 = sc.textFile(r"C:\data\retail_db\order_items\accumulator_broadcast_program.txt")

    broadcastVal1 = sc.broadcast(["a","e","i","o","u","z"])
    #broadcastval2 = sc.broadcast(file_1.collect())

    accum1 = sc.accumulator(0)
    #getWords = broadcastval2.value.flatMap(lambda b: b.split(" "))
    getAlphabets = file_1.flatMap(lambda c: c).collect()

    for d in getAlphabets:
        for e in broadcastVal1.value:
            if d == e:
                accum1 += 1
            else:
                accum1

    print("The total vowels in file is :" + str(accum1.value))

if __name__ == '__main__':
    main()
