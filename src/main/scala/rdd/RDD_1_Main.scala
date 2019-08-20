package rdd

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 主程序入口：两种方式
  *
  * written by Brain on 2019/08/02
  */
object RDD_1_Main extends App {
  // 方式一
  val spark = SparkSession
    .builder()
    .appName("主程序入口方式一")
    .master("local[2]")       // 设置url，本地 或者 standalone cluster，local[核数]
    .enableHiveSupport()              // 支持 hive
    .getOrCreate()
    .sparkContext

  // 方式二
  val spark1 = new SparkContext(
    new SparkConf()
      .setAppName("主程序入口方式二")
      .setMaster("spark://master:7077")
  )

  val spark2 = SparkContext.getOrCreate(
    new SparkConf()
      .setAppName("主程序入口方式二")
      .setMaster("local[*]")
  )
}
