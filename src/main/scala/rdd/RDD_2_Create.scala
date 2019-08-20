package rdd

import org.apache.spark.sql.SparkSession

/**
  * 创建 RDD 的方式
  *
  * written by Brain on 2019/08/02
  */
object RDD_2_Create extends App {
  val spark = SparkSession
    .builder()
    .appName("create rdd")
    .master("local[*]")
    .getOrCreate()
    .sparkContext

  // makeRDD、parallelize  设置 rdd 分区数
  val rdd1 = spark.makeRDD(1 to 10, 2)
  val rdd2 = spark.parallelize(List(1, 2, 3), 2)

  // 读取本地文件
  val rdd3 = spark.textFile("/Users/gengmei/IDEA/spark_study/src/main/scala/word.txt")
  // 读取 hdfs 文件
  val rdd4 = spark.hadoopFile("/user/hive/warehouse/online.db/al_community", 2)
}
