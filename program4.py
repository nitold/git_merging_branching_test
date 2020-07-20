from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

def spark_init():
    spark = SparkSession.builder \
        .appName("Program for dataframe broadcast and accumulator") \
        .master("local") \
        .getOrCreate()

    sc = spark.sparkContext
    spark.sparkContext.setLogLevel("ERROR")
    return spark,sc

def main():
    spark, sc = spark_init()

    schemaDF = StructType([StructField("empid",StringType(),True),
                           StructField("name",StringType(),True),
                           StructField("rank",DoubleType(),True),
                           StructField("salary",StringType(),True),
                           StructField("mgrid",StringType(),True)])

    empDfBr = spark.read\
    .option("header","true")\
    .schema(schemaDF)\
    .option("sep",",")\
    .format("csv")\
    .load(r"C:\data\retail_db\order_items\employee2.txt")

    #broadcastEmpDfBr = sc.broadcast(empDfBr)

    df1 = F.broadcast(empDfBr).alias("a").join(F.broadcast(empDfBr).alias("b"),\
          F.col("a.mgrid")==F.col("b.empid"),"leftouter")\
    .select(F.col("a.*"), F.col("b.name").alias("manager_name"))


    df1.withColumn("total_salary", F.sum(F.col("salary")).over(Window.partitionBy(F.col("manager_name"))))\
    .withColumn("rownum", F.row_number().over(Window.orderBy(F.col("total_salary").desc())))\
     .filter(F.col("rownum")==1).select("mgrid","manager_name").show()

    df1.groupBy(F.col("mgrid")).agg(F.max(F.col("salary")).alias("max_sal")).filter(F.col("mgrid").isNotNull())\
        .select("mgrid","max_sal").show()

if __name__ == '__main__':
    main()












