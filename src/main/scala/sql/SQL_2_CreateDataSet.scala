package sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

case class Person(name: String, age: BigInt)

object SQL_2_CreateDataset extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder()
    .appName("create DataSet")
    .master("local[*]")
    .getOrCreate()

  val person = spark.read.json("/Users/gengmei/IDEA/spark_study/src/main/scala/sql/people.json")

  import spark.implicits._

  List(Person("Andy", 15)).toDS().show()
  /*
    输出：
        +----+---+
        |name|age|
        +----+---+
        |Andy| 15|
        +----+---+
   */
  Seq(Person("Denny", 18)).toDS().show()
  /*
    输出：
        +-----+---+
        | name|age|
        +-----+---+
        |Denny| 18|
        +-----+---+
   */
  Seq(Person("Denny", 18)).toDF().show()
  /*
    输出：
        +-----+---+
        | name|age|
        +-----+---+
        |Denny| 18|
        +-----+---+
   */

  Seq("1","2","3").toDS().show()
  /*
    输出：
        +-----+
        |value|
        +-----+
        |    1|
        |    2|
        |    3|
        +-----+
   */
  Seq(1,1,2).toDS().map(_ + 1).show()
  /*
    输出：
        +-----+
        |value|
        +-----+
        |    2|
        |    2|
        |    3|
        +-----+
   */

  person.toDF().show()
  /*
    输出：
        +---+-------+
        |age|   name|
        +---+-------+
        | 19|Michael|
        +---+-------+
   */
  person.toDF("name", "age").show()
  /*
    输出：
        +---+-------+
        |age|   name|
        +---+-------+
        | 19|Michael|
        +---+-------+
   */
  person.as[Person].show()
  /*
    输出：
        +---+-------+
        |age|   name|
        +---+-------+
        | 19|Michael|
        +---+-------+
   */
}
