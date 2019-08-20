package rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object RDD_4_Pair_Action extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder()
    .appName("pair action rdd")
    .master("local[*]")
    .getOrCreate()
    .sparkContext

  val rdd1 = spark.parallelize(List((1, "a"), (2, "a"), (2, "b")), 2)

  /**
    * count(): Long
    * countByValue：与tuple元组中的（k,v）中的 v 没有关系，针对的是 rdd中 的每一个元素对象
    * countByKey
    */
  println(rdd1.count())
  // 输出：3
  println(rdd1.countByValue())
  // 输出：Map((2,a) -> 1, (1,a) -> 1, (2,b) -> 1)
  println(rdd1.countByKey())
  // 输出：Map(1 -> 1, 2 -> 2)


  /**
    * collectAsMap(): Map[K, V]
    */
  println(rdd1.collectAsMap())
  // 输出：Map(2 -> b, 1 -> a)     Map 的 k 不能重复
}
