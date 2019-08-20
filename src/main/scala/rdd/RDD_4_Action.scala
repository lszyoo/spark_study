package rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * 行动算子
  *
  * written by Brain on 2019/08/04
  */
object RDD_4_Action extends App {
  // 设置日志级别
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder()
    .appName("action rdd")
    .master("local")
    .getOrCreate()
    .sparkContext

  val rdd1 = spark.makeRDD(List(1, 2, 2, 9, 5), 2)
  val rdd2 = spark.makeRDD(1 to 10, 3)

  /**
    * foreachPartition - 1 个核对应 1 个分区，将一个函数应用到每个分区的 RDD 上
    * foreach(f: T => Unit): Unit
    */
  rdd1.foreachPartition(f => println(f.mkString(",")))
  /*
    输出：1,2
         2,9,5
   */
  rdd1.foreach(println)
  /*
    输出：1
         2
         2
         9
         5
   */

  /**
    * aggregate[U1: ClassTag](zeroValue: U1)(seqOp: (U1, T) => U2, combOp: (U1, U2) => U3): U3
    *
    * 三个参数，初始化值，然后根据初始化值进行对rdd中的元素进行聚合，结束之后每个分区会有一个结果，后面会根据这个分区结果再进行一次聚合
    *
    * 1.（1）第一个参数 U1 是第二个参数和第三个参数的初始值
    *   （2）T 是调用该方法的 RDD 的元素
    *   （3）第二个参数的结果 U2 是第三个参数的参数
    *   （4）返回值类型 U 同初始值 zeroValue 一样
    *
    * 2.（1）seqOp操作会在各分区中聚合本分区的元素，然后combOp操作把所有分区的聚合结果再次聚合，两个操作的初始值都是zeroValue.
    *   （2）seqOp的操作是遍历分区中的所有元素(T)，第一个T跟zeroValue做操作，结果再作为与第二个T做操作的zeroValue，直到遍历完整个分区
    *   （3）combOp操作是把各分区聚合的结果，再聚合。
    *   （4）aggregate函数返回一个跟RDD不同类型的值。因此，需要一个操作seqOp来把分区中的元素T合并成一个U，另外一个操作combOp把所有U聚合
    *
    * 例子：演示单线程计算过程，实际Spark执行中是分布式计算，可能会把List分成多个分区，假如3个，p1(1,2,3,4)，p2(5,6,7,8)，p3(9,10)，
    * 经过计算各分区的的结果(10,4),(26,4),(19,2)，这样，执行(m,n) => (m._1+n._1,m._2+n._2)就是(10+26+19,4+4+2）即（55,10）
    */
  println(rdd1.aggregate(0)((_ + _), (_ + _)))
  // 输出：19
  val agg = rdd1.aggregate(List[Int]())(
    (m, n) => m.::(n),
    (m, n) => m.:::(n)
  )
  println(agg)
  // 输出：List(5, 9, 2, 2, 1)
  println(rdd2.aggregate((0,0))((x,y) => (x._1 + y, x._2 + 1), (m,n) => (m._1 + n._1, m._2 + n._2)))
  // 输出：(55,10)

  /**
    * fold(zeroValue: T)(op: (T, T) => T): T
    *
    * 第一个分区：1 + 1 + 2 = 4
    * 第二个分区：1 + 2 + 9 + 5 = 17
    * 聚合：1 + 4 + 17 = 22
    *
    * reduce
    */
  println(rdd1.fold(1)(_ + _))
  // 输出：22
  println(rdd1.reduce(_ + _))
  // 输出：19

  /**
    * countByValue
    * countByValueApprox
    * count：返回 RDD 中元素的个数
    * countApprox(timeout: Long, confidence: Double = 0.95): PartialResult[BoundedDouble]
    * countApproxDistinct：快速估算不重复个数
    */
  println(rdd1.countByValue())
  // 输出：Map(2 -> 2, 1 -> 1, 9 -> 1, 5 -> 1)
  println(rdd1.count())
  // 输出：5
  println(rdd1.countApprox(2, 0.9))
  // 输出：(partial: [0.000, Infinity])
  println(rdd1.countByValueApprox(2, 0.95))
  // 输出：(partial: Map())

  /**
    * checkpoint：设立检查点（hdfs）
    */
  spark.setCheckpointDir("/Users/gengmei/checkpoint")
  rdd1.checkpoint()

  /**
    * context: SparkContext
    * dependencies
    */
  rdd1.context.makeRDD(List(3, 4), 2).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：3
         4
   */
  println(rdd1.map((_, 1)).reduceByKey(_ + _).dependencies.toList.mkString(","))
  // 输出：org.apache.spark.ShuffleDependency@e1e2e5e


  /**
    * first
    */
  println(rdd1.first())
  // 输出：1

  /**
    * getCheckpointFile: Option[String]    获取checkpoint的文件
    * getNumPartitions: Int     获取分区个数
    * getStorageLevel: StorageLevel   获取缓存级别
    *
    * id   获取 rdd 编号
    */
  println(rdd1.getCheckpointFile)
  // 输出：None
  println(rdd1.getNumPartitions)
  // 输出：2
  println(rdd1.getStorageLevel)
  // 输出：StorageLevel(1 replicas)
  println(rdd1.id)
  // 输出：0

  /**
    * isCheckpointed
    * isEmpty
    */
  println(rdd1.isCheckpointed)
  // 输出：false
  println(rdd1.isEmpty())
  // 输出：false

  /**
    * toLocalIterator: Iterator[T]     返回一个包含 rdd 所有元素的迭代器
    *
    * iterator
    */
  println(rdd1.toLocalIterator.toList)
  // 输出：List(1, 2, 2, 9, 5)

  /**
    * max
    * min
    */
  println(rdd1.max())
  // 输出：9
  println(rdd1.min())
  // 输出：1

  /**
    * partitioner: Option[Partitioner]
    * partitions: Array[Partition]
    * preferredLocations(split: Partition): Seq[String]
    */
  println(rdd1.partitioner)
  // 输出：None
  println(rdd1.partitions)
  // 输出：[Lorg.apache.spark.Partition;@77e7246b
  println(rdd1.preferredLocations(rdd1.partitions(0)))
  // 输出：List()


  /**
    * saveAsObjectFile  按分区保存为二进制对象文件
    * saveAsTextFile    按分区保存为text文件
    */
  rdd1.saveAsObjectFile("/Users/gengmei/checkpoint/object")
  rdd1.saveAsTextFile("/Users/gengmei/checkpoint/text")

  /**
    * sparkContext
    */
  rdd1.sparkContext

  /**
    * take  用于获取RDD中从0到num-1下标的元素，不排序
    * takeOrdered  用于获取RDD中从0到num-1下标的元素，排序
    * takeSample
    * top   与 takeOrdered 相反
    */
  println(rdd1.take(2).toList)
  // 输出：List(1, 2)
  println(rdd1.takeOrdered(4).toList)
  // 输出：List(1, 2, 2, 5)
  println(rdd1.takeSample(true, 2, 2).toList)
  // 输出：List(2, 5)
  println(rdd1.top(3).toList)
  // 输出：List(9, 5, 2)

  /**
    * toDebugString   rdd 相关信息
    * toString
    */
  println(rdd1.toDebugString)
  // 输出：(2) ParallelCollectionRDD[0] at makeRDD at RDD_4_Action.scala:20 []
  println(rdd1.toString())
  // 输出：ParallelCollectionRDD[0] at makeRDD at RDD_4_Action.scala:20

  /**
    * treeAggregate
    * treeReduce
    * aggregate 会把分区的结果直接拿到driver端做reduce操作。treeAggregate会先把分区结果做reduceByKey，
    * 最后再把结果拿到driver端做reduce,算出最终结果。reduceByKey需要几层，由参数depth决定，也就是相当于做了depth层的reduceByKey
    * aggregate把数据全部拿到driver端，存在内存溢出的风险。treeAggregate则不会。
    */
  println(rdd1.treeAggregate(1)(_ + _, _ + _, 2))
  // 输出：24
  println(rdd1.treeReduce(_ + _, 2))
  // 输出：19
}
