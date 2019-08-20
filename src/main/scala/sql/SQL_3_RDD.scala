package sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * RDD 的互操作性
  */
object SQL_3_RDD extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder()
    .appName("rdd 互操性")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  /**
   * 使用反射推断 Schema
   * Spark SQL 的 Scala 接口支持自动转换一个包含 case classes 的 RDD 为 DataFrame。Case class 定义了表的 Schema。
   * Case class 的参数名使用反射读取并且成了列名。Case class 也可以是嵌套的或者包含像 SeqS 或者 ArrayS 这样的复杂类型。
   * 这个 RDD 能够被隐式转换成一个 DataFrame 然后被注册为一个表。表可以用于后续的 SQL 语句。
   */
  import spark.implicits._

  val person = sc
    .textFile("/Users/gengmei/IDEA/spark_study/src/main/scala/sql/people.txt")
    .map(_.split(","))
    .map(person => Person(person(0), person(1).trim.toInt))
    .toDF()

  person.createTempView("person")

  val teenagers = spark.sql("select * from person where age < 23")

  teenagers.show()
  /*
    输出：
        +-----+---+
        | name|age|
        +-----+---+
        |Denny| 20|
        +-----+---+
   */
  teenagers.map(_(0).toString).show()
  /*
    输出：
        +-----+
        |value|
        +-----+
        |Denny|
        +-----+
   */
  teenagers.map(_.getAs[String]("name")).show()
  /*
   输出：
       +-----+
       |value|
       +-----+
       |Denny|
       +-----+
  */

  implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
  teenagers.map(_.getValuesMap[Any](List("name", "age"))).collect().foreach(println)
  // 输出：Map(name -> Denny, age -> 20)


  /**
   * 以编程的方式指定 Schema
   * 当 case class 不能够在执行之前被定义（例如，records 记录的结构在一个 string 字符串中被编码了，或者一个 text 文本 dataset
   * 将被解析并且不同的用户投影的字段是不一样的）。一个 DataFrame 可以使用下面的三步以编程的方式来创建。
   * 从原始的 RDD 创建 RDD 的 RowS（行）。
   * Step 1 被创建后，创建 Schema 表示一个 StructType 匹配 RDD 中的 Rows（行）的结构。
   * 通过 SparkSession 提供的 createDataFrame 方法应用 Schema 到 RDD 的 RowS（行）。
   */
  // The schema is encoded in a string
  val schemaString = "name age"

  // Generate the schema based on the string of schema
  val fields = schemaString.split(" ")
    .map((StructField(_, StringType, nullable = false)))
  val schema = StructType(fields)

  val rowRDD = sc
    .textFile("/Users/gengmei/IDEA/spark_study/src/main/scala/sql/people.txt")
    .map(_.split(","))
    .map(person => Row(person(0), person(1).trim))


  val personDF = spark.createDataFrame(rowRDD, schema)
  personDF.show()
  /*
    输出：
        +-----+---+
        | name|age|
        +-----+---+
        |Denny| 20|
        | Andy| 25|
        | Mark| 23|
        +-----+---+

   */

  //创建一个元素是 Json 的 RDD，用spark SQL 读取
  val personRDD = sc.makeRDD("""{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil).toDS()
  spark.read.json(personRDD).show()
  /*
    输出：
        +----------------+----+
        |         address|name|
        +----------------+----+
        |[Columbus, Ohio]| Yin|
        +----------------+----+
   */
}
