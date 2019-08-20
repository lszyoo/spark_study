package rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * 转换算子
  *
  * written by Brain on 2019/08/04
  */
object RDD_3_Transformation extends App {
  // 设置日志级别
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder()
    .appName("transformation rdd")
    .master("local[*]")
    .getOrCreate()
    .sparkContext

  val rdd1 = spark.parallelize(1 to 5, 2)
  val rdd2 = spark.parallelize(6 to 10, 2)
  val rdd3 = spark.parallelize(List(("a", 1), ("a", 2), ("b", 4), ("c", 6)), 2)
  val rdd4 = spark.makeRDD(Array(2, 4, 2, 4, 8), 2)
  val rdd5 = spark.parallelize(List("Hello World", "Hello Java"), 2)


  /**
    * ++ 等同 union 窄依赖， 合并两个 rdd
    * 注意：返回分区数也做了加和
    */
  (rdd1 ++ rdd2).foreachPartition(f => println(f.mkString(",")))
  rdd1.union(rdd2).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：8,9,10
         3,4,5
         6,7
         1,2
   */
  println(rdd1.union(rdd2).getNumPartitions)
  // 输出：4


  /**
    * cache、persist 缓存，其中 persist 可以设置级别，默认为 cache
    * unpersist 解除缓存
    */
  rdd1.persist(StorageLevel.DISK_ONLY)
  rdd1.unpersist()


  /**
    * reparation 的分区结果比较的随意,没有什么规律
    * partitionBy 把相同的key都分到了同一个分区
    * coalesce 重新设置分区操作，注意，有两个参数，一个分区个数，true 和 false 代表是否shuffle
    * 注意：partitionBy 只能用于 (k,v) 形式的 RDD
    */
  rdd3.coalesce(3, true).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：(a,2),(b,4)
         (c,6)
         (a,1)
   */
  rdd3.repartition(4).glom().collect().toList.foreach(arr => println(arr.mkString(",")))
  /*
    输出：(b,4)
         (a,2)
         (c,6)
         (a,1)
   */
  rdd3.partitionBy(new HashPartitioner(4)).glom().collect().toList.foreach(arr => println(arr.mkString(",")))
  /*
    输出：空
         (a,1),(a,2)
         (b,4)
         (c,6)
   */


  /**
    * cartesian 笛卡尔积：分区数相乘，每个分区的数字互相组合
    */
  rdd1.cartesian(rdd2).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：(3,8),(3,9),(3,10),(4,8),(4,9),(4,10),(5,8),(5,9),(5,10)
         (3,6),(3,7),(4,6),(4,7),(5,6),(5,7)
         (1,6),(1,7),(2,6),(2,7)
         (1,8),(1,9),(1,10),(2,8),(2,9),(2,10)
   */

  /**
    * collect 根据一个偏函数返回一个符合偏函数的结果集RDD
    */
  val s: PartialFunction[Int, String] = {
    case 1 => "one"
    case _ => "other"
  }
  rdd1.collect(s).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：other,other,other
         one,other
   */


  /**
    * distinct(去重)：有参数 - 重新指定分区数
    *                无参数 - 默认分区数
    */
  rdd4.distinct().foreachPartition(f => println(f.mkString(",")))
  /*
    输出：空
         4,8,2
   */
  rdd4.distinct(3).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：空
         8,2
         4
   */


  /**
    * filter 过滤，根据里面的规则返回一个过滤过后的rdd,参数时boolean类型
    */
  rdd1.filter(_ > 3).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：空
         4,5
   */


  /**
    * flatMap 一对多
    * map 一对一
    * mapPartitions 该函数和 map 函数类似，只不过映射函数的参数由 RDD 中的每一个元素变成了 RDD
    *     中每一个分区的迭代器。如果在映射的过程中需要频繁创建额外的对象，使用 mapPartitions 要比 map高效的多。
    * mapPartitionsWithIndex 多了个分区索引
    */
  rdd5.flatMap(_.split(" ")).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：Hello,Java
         Hello,World
   */
  rdd5.map((_, 1)).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：(Hello Java,1)
         (Hello World,1)
   */
  rdd1.mapPartitions{
    x => {
      var result = List[Int]()
      var i = 0
      while (x.hasNext)
        i += x.next()
      result.::(i).iterator
    }
  }.foreachPartition(f => println(f.mkString(",")))
  /*
    输出：将每个分区累加
         3
         12
   */
  rdd1.mapPartitionsWithIndex {
    (x, iter) => {
      var result = List[Tuple2[Int, Int]]()
      var i = 0
      while (iter.hasNext)
        i += iter.next()
      // result.::(x + "|" + i).iterator
      result.::((x, i)).iterator
    }
  }.foreachPartition(f => println(f.mkString(",")))
  /*
    输出：(0,3)      分区编号，每个分区累加值
         (1,12)
   */


  /**
    * glom 将 rdd 分区元素变成一个数组元素
    */
  println(rdd2.glom().count())
  // 输出：2


  /**
    * groupBy 根据自己定义的规则来划分组
    */
  rdd1.groupBy(_ % 2).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：(0,CompactBuffer(2, 4))
         (1,CompactBuffer(1, 3, 5))
   */
  rdd1.groupBy((f: Int) => f % 2, 3).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：空
         (1,CompactBuffer(1, 3, 5))
         (0,CompactBuffer(2, 4))
   */
  rdd1.groupBy((f: Int) => f % 2, new HashPartitioner(3)).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：(1,CompactBuffer(1, 3, 5))
         (0,CompactBuffer(2, 4))
         空
   */


  /**
    * intersection 返回两个rdd相同部分的数据集合
    */
  rdd1.intersection(rdd4).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：空
         4,2
   */
  rdd1.intersection(rdd4, 1).foreachPartition(f => println(f.mkString(",")))
  // 输出：4,2
  rdd1.intersection(rdd4, new HashPartitioner(1)).foreachPartition(f => println(f.mkString(",")))
  // 输出：4,2


  /**
    * keyBy 将每个元素组成 kv 对
    */
  rdd1.keyBy(_ => "a").foreachPartition(f => println(f.mkString(",")))
  /*
    输出：(a,3),(a,4),(a,5)
         (a,1),(a,2)
   */

  /**
    * pipe 执行脚本
    */
  rdd1.pipe("head -n 1").foreachPartition(f => println(f.mkString(",")))
  /*
    输出：3      取每个分区的第一个元素
         1
   */


  /**
    * sample 抽样
    * 参数：
    *   1、withReplacement：元素可以多次抽样(在抽样时替换)
    *
    *   2、fraction：期望样本的大小作为RDD大小的一部分
    *    当withReplacement=false时：选择每个元素的概率，分数一定是[0,1]
    *    当withReplacement=true时：选择每个元素的期望次数，分数必须大于等于 0
    *
    *   3、seed：随机数生成器的种子
    */
  rdd1.sample(true, 1).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：4
         1
   */

  /**
    * setName 设置rdd名称，不设置返回 null
    */
  println(rdd1.name)
  // 输出：null
  println(rdd1.setName("num").name)
  // 输出：num


  /**
    * sortBy 根据规则来定义排序, true 或 false 控制升序或者降序,第一个参数控制排序逻辑
    */
  rdd3.sortBy(_._2, false, 2).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：(a,2),(a,1)
         (c,6),(b,4)
   */


  /**
    * subtract 返回 rdd1 中去掉 rdd4与rdd1 相交的元素
    */
  rdd1.subtract(rdd4).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：1,3,5
         空
   */
  rdd1.subtract(rdd4, 1).foreachPartition(f => println(f.mkString(",")))
  // 输出：1,3,5
  rdd1.subtract(rdd4, new HashPartitioner(2)).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：空
         1,3,5
   */


  /**
    * toJavaRDD
    */
  println(rdd1.toJavaRDD().first())
  // 输出：1


  /**
    * zip 将两个rdd转换成k/v类型的rdd，注意两个rdd必须保持元素个数和分区数一致
    * zipPartitions 该函数是将多个RDD按照partitions组合成新的RDD，要求组合的RDD具有相同的分区数，各分区的元素数量可以不同
    * zipWithIndex  该函数将RDD中的元素和这个元素在RDD中的ID（索引号）组合成键/值对
    * zipWithUniqueId  该函数将RDD中元素和一个唯一ID组合成键/值对，该唯一ID生成算法如下：
    *                  每个分区中第一个元素的唯一ID值为：该分区索引号
    *                  每个分区中第N个元素的唯一ID值为：(前一个元素的唯一ID值) + (该RDD总的分区数)
    */
  rdd1.zip(rdd2).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：(3,8),(4,9),(5,10)
         (1,6),(2,7)
   */
  rdd1.zipPartitions(rdd2){
    (it1, it2) => {
      var result = List[String]()
      while (it1.hasNext && it2.hasNext)
        result ::= (it1.next() + "_" + it2.next())
      result.iterator
    }
  }.foreachPartition(f => println(f.mkString(",")))
  /*
    输出：2_7,1_6
         5_10,4_9,3_8
   */
  rdd1.zipPartitions(rdd2, false){    // 参数 是否保留分区
    (it1, it2) => {
      var result = List[String]()
      while (it1.hasNext && it2.hasNext)
        result ::= (it1.next() + "_" + it2.next())
      result.iterator
    }
  }.foreachPartition(f => println(f.mkString(",")))
  /*
    输出：2_7,1_6
         5_10,4_9,3_8
   */
  rdd1.zipWithIndex().foreachPartition(f => println(f.mkString(",")))
  /*
    输出：(1,0),(2,1)
         (3,2),(4,3),(5,4)
   */
  rdd1.zipWithUniqueId().foreachPartition(f => println(f.mkString(",")))
  /*
    输出：(1,0),(2,2)
         (3,1),(4,3),(5,5)
   */
}
