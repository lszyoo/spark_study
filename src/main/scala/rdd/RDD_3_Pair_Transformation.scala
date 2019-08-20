package rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object RDD_3_Pair_Transformation extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder()
    .appName("pair rdd transformation")
    .master("local[*]")
    .getOrCreate()
    .sparkContext

  val rdd1 = spark.parallelize(List(("a", 1), ("a", 2), ("b", 4), ("c", 6), ("a", 1), ("a", 1)), 2)
  val rdd2 = spark.parallelize(List((3, 1), (4, 2), (2, 4), (1, 6), (3, 1), (3, 1)), 2)
  val rdd3 = spark.parallelize(List((3, "a"), (2, "b"), (5, "c")), 2)
  val rdd4 = spark.parallelize(List((1, "Java,Scala"), (2, "Spark,Flink")), 2)

  /**
    * groupByKey
    * combineByKey
    * aggregateByKey：每个分区的每个key都有一个初始值，zeroValue在第三参数不参与，再聚合
    * reduceByKey
    */
  rdd1.groupByKey().foreachPartition(f => println(f.mkString(",")))
  /*
    输出：(b,CompactBuffer(4))
         (a,CompactBuffer(1, 2, 1, 1)),(c,CompactBuffer(6))
   */
  rdd1.combineByKey(
    List(_),
    (x: List[Int], y: Int) => y :: x,
    (x: List[Int], y: List[Int]) => x ::: y
  ).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：(b,List(4))
         (a,List(2, 1, 1, 1)),(c,List(6))
   */
  rdd1.partitionBy(new HashPartitioner(2)).aggregateByKey("x")(
    (x, y) => x + y,
    (m, n) => m + n
  ).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：(a,x1211),(c,x6)
         (b,x4)
   */
  rdd1.aggregateByKey("x")(
    (x, y) => x + y,
    (m, n) => m + n
  ).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：(b,x4)
         (a,x12x11),(c,x6)
    运行过程：
         a, 1
         a, 2                a, x12             a, x12x11
         c, 6     a, 12      c, x6              c, x6
                 ======>                ==>
         b, 4     a, 11      b, 4               b, 4
         a, 1                a, x11
         a, 1
   */
  rdd2.aggregateByKey(1)(
    (a, b) => math.max(a, b),
    (a, b) => a + b
  ).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：(1,6),(3,2)
         (4,2),(2,4)
   */
  rdd1.reduceByKey(_ + _).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：(a,5),(c,6)
         (b,4)
   */


  /**
    * cogroup 相当于SQL中的全外关联full outer join，返回左右RDD中的记录，关联不上的为空。
    * join 相当于SQL中的内关联join，只返回两个RDD根据K可以关联上的结果，join只能用于两个RDD之间的关联，
    *      如果要多个RDD关联，多关联几次即可。
    * fullOuterJoin
    * rightOuterJoin
    */
  rdd2.cogroup(rdd3).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：(4,(CompactBuffer(2),CompactBuffer())),(2,(CompactBuffer(4),CompactBuffer(b)))
         (1,(CompactBuffer(6),CompactBuffer())),(3,(CompactBuffer(1, 1, 1),CompactBuffer(a))),(5,(CompactBuffer(),CompactBuffer(c)))
   */
  rdd2.join(rdd3).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：(2,(4,b))
         (3,(1,a)),(3,(1,a)),(3,(1,a))
   */
  rdd2.fullOuterJoin(rdd3).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：(4,(Some(2),None)),(2,(Some(4),Some(b)))
         (1,(Some(6),None)),(3,(Some(1),Some(a))),(3,(Some(1),Some(a))),(3,(Some(1),Some(a))),(5,(None,Some(c)))
   */
  rdd2.rightOuterJoin(rdd3).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：(2,(Some(4),b))
         (3,(Some(1),a)),(3,(Some(1),a)),(3,(Some(1),a)),(5,(None,c))
   */
  rdd2.leftOuterJoin(rdd3).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：(4,(2,None)),(2,(4,Some(b)))
         (1,(6,None)),(3,(1,Some(a))),(3,(1,Some(a))),(3,(1,Some(a)))
   */


  /**
    * foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)]
    */
  rdd3.foldByKey("x")(_ + _).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：(2,xb)
         (3,xa),(5,xc)
   */

  /**
    * flatMapValues[U](f: V => TraversableOnce[U]): RDD[(K, U)]
    */
  rdd4.flatMapValues(_.split(",")).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：(1,Java),(1,Scala)
         (2,Spark),(2,Flink)
   */

  /**
    * mapValues[U](f: V => U): RDD[(K, U)]
    */
  rdd4.mapValues(_.length).foreachPartition(f => println(f.mkString(",")))
  /*
    输出：(1,10)
         (2,11)
   */
}
