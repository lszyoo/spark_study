package sql

import org.apache.spark.sql.SparkSession

object SQL_5_Hive extends App {
  val warehouseLocation="hdfs://62.234.191.59:8020/user/hive/warehouse"

  val spark = SparkSession.builder()
    .appName("SparkHive")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .master("local[4]")
    .enableHiveSupport()
    .getOrCreate()

  val sc = spark.sparkContext

  import spark.sql

  val sqlDF = sql("SELECT COUNT(*) FROM online.al_device_push_category WHERE partition_date = '20190101'")
  sqlDF.show(10000)

  spark.stop()
}
