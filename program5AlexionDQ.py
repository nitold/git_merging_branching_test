from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def spark_init():
    spark = SparkSession.builder\
    .appName("Alexion DQ program")\
    .master("local[2]")\
    .getOrCreate()

    sc = spark.sparkContext
    spark.sparkContext.setLogLevel("ERROR")
    return spark, sc

def main():
    spark, sc = spark_init()

    df1 = spark.read.format("csv")\
    .option("header","true")\
    .option("sep",",")\
    .option("inferSChema","true")\
    .load(r"C:\data\retail_db\order_items\employee.txt")

    df1.createOrReplaceTempView("employee_df")

    df2 = spark.read.format("csv")\
    .option("header","true")\
    .option("sep","|")\
    .option("inferSChema","true")\
    .load(r"C:\data\retail_db\order_items\alexionDQ.txt")

    id1 = 1
    id2 = 2

    query1 = str(df2.select("query").filter(F.col("id")==id1).collect()).replace("[Row(query='","").replace("')]","")
    query2 = str(df2.select("query").filter(F.col("id")==id2).collect()).replace("[Row(query='","").replace("')]","")

    spark.sql("{0}".format(query1)).show()
    spark.sql("{0}".format(query2)).show()

if __name__ == '__main__':
    main()