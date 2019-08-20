package sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SQL_1_CreateDataFrame extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder()
    .appName("create DataFrame")
    .master("local[*]")
    .getOrCreate()

  // json 不能格式化，否则报错
  val person = spark.read.json("/Users/gengmei/IDEA/spark_study/src/main/scala/sql/people.json")

  person.show()
  /*
    输出：
        +---+-------+
        |age|   name|
        +---+-------+
        | 19|Michael|
        +---+-------+
   */
  person.printSchema() // 打印一个tree
  /*
    输出：
        root
         |-- age: long (nullable = true)
         |-- name: string (nullable = true)
   */
  person.select("name").show() // 取一列数据
  /*
    输出：
        +-------+
        |   name|
        +-------+
        |Michael|
        +-------+
   */
  person.select("name", "age").show() // 取多列数据
  /*
    输出：
        +-------+---+
        |   name|age|
        +-------+---+
        |Michael| 19|
        +-------+---+
   */
  person.select(
    person.col("name"),
    (person.col("age") + 1).as("age")
  ).show()
  /*
    输出：
        +-------+---+
        |   name|age|
        +-------+---+
        |Michael| 20|
        +-------+---+
   */
  person.filter(person.col("age") > 18).show()
  /*
    输出：
        +---+-------+
        |age|   name|
        +---+-------+
        | 19|Michael|
        +---+-------+
   */
  person.groupBy("name").count().as("count").show()
  /*
    输出：
        +-------+-----+
        |   name|count|
        +-------+-----+
        |Michael|    1|
        +-------+-----+
   */

  /**
    * 创建临时表
    *
    * createTempView：此视图的生命周期依赖于SparkSession类
    * createOrReplaceTempView
    * createGlobalTempView：此视图的生命周期取决于spark application本身
    * createOrReplaceGlobalTempView
    */
  person.createTempView("person1")
  spark.sql("select * from person1").show()
  /*
    输出：
        +---+-------+                        
        |age|   name|                        
        +---+-------+                        
        | 19|Michael|                        
        +---+-------+
    */
  person.createOrReplaceTempView("person1")
  spark.sql("select * from person1").show()
  /*
  输出：
      +---+-------+
      |age|   name|
      +---+-------+
      | 19|Michael|
      +---+-------+
 */
  // 关闭临时表两种方式
  spark.catalog.dropTempView("person1")
  // spark.stop()

  person.createGlobalTempView("person2")
  spark.sql("select * from global_temp.person2").show()
  /*
    输出：
        +---+-------+
        |age|   name|
        +---+-------+
        | 19|Michael|
        +---+-------+
   */
  person.createOrReplaceGlobalTempView("person2")
  spark.sql("select * from global_temp.person2").show()
  /*
  输出：
      +---+-------+
      |age|   name|
      +---+-------+
      | 19|Michael|
      +---+-------+
 */
  spark.catalog.dropGlobalTempView("person2")
  spark.stop()
}