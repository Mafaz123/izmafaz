class Promo:
   'Common base class for all employees'
   promoval = 0
   def __init__(self, custid, amount):
      self.custid = custid
      self.amount = amount
      Promo.promoval += amount + 100
   def promoapply(self):
      return promoval

obj1=Promo(1,10000)

from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
if __name__ == '__main__':
   #define spark configuration object
   conf = SparkConf().setAppName("Local-sparkcore").setMaster("local[*]")
   spark = SparkSession.builder.master("local[1]").appName("Spark core pyspark")                                                                             .getOrCreate()
   #Set the logger level to error
   spark.sparkContext.setLogLevel("ERROR")
   # Create a file rdd with 4 partitions
   rdd = sc.textFile("file:/home/hduser/hive/data/txns",4)
   print(rdd.count())
   # create a hadoop rdd with 4 partitions
   # create the below dir in hadoop and place the file txns in the below dir
   #hadoop fs -mkdir -p /user/hduser/hive/data/
   #hadoop fs -put -f ~/hive/data/txns hive/data/
   hadooprdd = sc.textFile("file:/home/hduser/hive/data/txns",4                                                                             )
   print('total number of lines in hadoop - ' + str(hadooprdd.count()));
   print('print a sample of 20 rows with seed of 100 for dynamic data');
   for x in rdd.take(10) :
      print(x)
   print("print only the first line of the dataset " );
   print(rdd.first());
   print('type of rdd is ' + str(type(rdd)));
   print('Number of partition of the base file is ' + str(rdd.getNumPartitions()                                                                             ));
   #Create a splitted rdd to split the line of string to line of array
   rddsplit=rdd.map(lambda x:x.split(","))
