import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends App {
    // 只打印错误日志
    Logger.getLogger("org").setLevel(Level.ERROR)

    /**
      * 两种主程序入口
      */
    // 方式一：
    val sc = new SparkConf().setAppName("wordcount").setMaster("local[2]")
    // 两种方式
    val spark = new SparkContext(sc)
    val spark1 = SparkContext.getOrCreate(sc)

    // 方式二：
    val spark2 = SparkSession
      .builder()
      .appName("WordCount")
      .master("local[2]")
      .getOrCreate()
      .sparkContext

    println("=== 读取自建 RDD ===")

    val rdd = spark.makeRDD(List("Hello World", "Hello Java", "Java C"))
    // 持久化到内存
    val cacheRDD = rdd.cache()
    cacheRDD.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).foreach(println)
    /*
       输出：
            (Hello,2)
            (Java,2)
            (World,1)
            (C,1)
     */

    println("=== 以下读取文件 ===")

    val textRDD = spark.textFile("/Users/gengmei/IDEA/spark_study/src/main/scala/word.txt")
    textRDD
      .flatMap(_.replaceAll("\n", " ").split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .foreach(println)
    /*
        输出：
            (percent,16)
            (in,,1)
            (14.2,1)
            (high-tech,4)
            (respectively.,1)
            ... ...
     */
}
