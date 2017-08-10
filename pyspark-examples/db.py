from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

class DB(object):
  def __init__(self):
      self.spark = SparkSession.builder.appName("spark").getOrCreate()
      self.conf = (
      SparkConf()
        .setAppName("s3a_test")
        .set("spark.executor.instances", "8")
        .set("spark.executor.cores", 2)
        .set("spark.shuffle.compress", "true")
        .set("spark.io.compression.codec", "snappy")
        .set("spark.executor.memory", "2g")
      )
      self.sc = SparkContext().getOrCreate(conf = self.conf)
      self.sc.setLogLevel("ERROR")
      self.sqlContext = SQLContext(self.sc)
      
  def get_mysql_data(self):
      url = "jdbc:mysql://server:port/schema?user=<username>&password=<password>"
      df = self.sqlContext\
        .read.format("jdbc")\
        .option("url", url)\
        .option("dbtable", "my_awesome_table")\
        .load()
      return df
  
  def get_pgsql(self):
        url = "jdbc:postgresql://server:port/schema"
        sql_query = """ (SELECT * FROM sandbox.awesome_table) AS dmap """
        
        df = self.spark.read.format("jdbc") \
                  .option("url", url) \
                  .option("dbtable", sql_query) \
                  .option("user", "<username>") \
                  .option("password", "<password>") \
                  .option("driver", "org.postgresql.Driver") \
                  .load()

        df.printSchema()
        return df
        
if __name__ == '__main__':
    DB().get_mysql_data()
