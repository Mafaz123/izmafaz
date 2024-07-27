from pyspark.sql.functions import *
from pyspark.sql.types import *
def main():
   from pyspark.sql import SparkSession
   # define spark configuration object
   spark = SparkSession.builder\
      .appName("AWS S3 Read/Write") \
      .enableHiveSupport()\
      .getOrCreate()
   sc=spark.sparkContext
   sc.setLogLevel("ERROR")
   #hadoop fs -mkdir -p s3a://inceptez-hdfs-bucket/custdata
   #hadoop fs -put ~/hive/data/custs s3a://inceptez-hdfs-bucket/custdata
   print("Use Spark Application to Read csv data from cloud S3, join and convert the data into Parquet and load into S3 partition location for Athena table ")
   custstructtype = StructType([StructField("id", IntegerType(), False),
                              StructField("fname", StringType(), False),
                              StructField("lname", StringType(), True),
                              StructField("age", ShortType(), True),
                              StructField("profession", StringType(), True)])
   s3_df_custs = spark.read.schema(custstructtype).option("delimiter", ",")\
   .csv("s3a://inceptez-hdfs-bucket/custdata/")
   print("S3 Read cust Completed Successfully")
   s3_df_custs.show(2)

   s3_df_txns = spark.read.option("inferschema", True) \
      .option("header", False).option("delimiter", ",") \
      .csv("s3a://inceptez-hdfs-bucket/txndata/txns") \
      .toDF("txnid", "dt", "custid", "amt", "category", "product", "city", "state", "transtype").na.drop("all")
   print("S3 Read txns Completed Successfully")
   s3_df_txns.show(2)
   #transformation, enrichment
   txns=s3_df_txns.withColumn("dt", to_date("dt", 'MM-dd-yyyy')).withColumn("yr", year("dt"))
   #denormalization, join
   denormalized_df=s3_df_custs.join(txns, on=[col("id") == col("custid")], how="inner")
   denormalized_df.show(2)
   denormalized_df.withColumn("loaddt",current_date()).write.partitionBy("loaddt").mode("overwrite").parquet("s3a://inceptez-hdfs-bucket/athenadata_modified")
   print("S3 Partition Write Completed Successfully")
   denormalized_df.write.mode("overwrite").saveAsTable("default.cust_transaction")
   print("EMR Hive table Write Completed Successfully")
main()
