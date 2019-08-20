package sql

import java.io.FileInputStream
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 通用的 Load/Save 函数
  *
  * 先读取文件，并将取到的内容保存到本地文件中
  *
  * SaveMode.ErrorIfExists(default)	 "error" (default)
  *          在保存 DataFrame 到一个数据源时，如果数据已经存在，将会抛出异常。
  * SaveMode.Append	"append"
  *          在保存 DataFrame 到一个数据源时，如果数据/表已经存在，DataFrame 的内容将会追加到已存在的数据后面。
  * SaveMode.Overwrite	"overwrite"	Overwrite
  *          模式意味着当保存 DataFrame 到一个数据源，如果数据/表已经存在，那么已经存在的数据将会被 DataFrame 的内容覆盖。
  * SaveMode.Ignore	"ignore"	Ignore
  *          模式意味着当保存 DataFrame 到一个数据源，如果数据已经存在，save 操作不会将 DataFrame 的内容保存，
  *          也不会修改已经存在的数据。这个和 SQL 中的 'CREATE TABLE IF NOT EXISTS' 类似。
  *
  */
object SQL_4_LoadSave extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder()
    .appName("load and save")
    .master("local[*]")
    .getOrCreate()

  // 设置目录
  val spark1 = SparkSession
    .builder()
    .appName("jdbc")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "spark-warehouse")
    .getOrCreate()

  val personDF = spark.read.format("json").load("/Users/gengmei/IDEA/spark_study/src/main/scala/sql/people.json")
  personDF.show()
  /*
    输出：
        +---+-------+
        |age|   name|
        +---+-------+
        | 19|Michael|
        +---+-------+
   */
  personDF.select("name").write.format("parquet").mode("append").save("/Users/gengmei/IDEA/spark_study/src/main/scala/sql/save_parquet")

  // 默认数据源类型 parquet
  val default = spark.read.load("/Users/gengmei/IDEA/spark_study/src/main/scala/sql/save_parquet")
  default.show()
  /*
    输出：
        +-------+
        |   name|
        +-------+
        |Michael|
        +-------+
   */

  spark.sql("select * from parquet.`/Users/gengmei/IDEA/spark_study/src/main/scala/sql/save_parquet`").show()
  /*
  输出：
      +-------+
      |   name|
      +-------+
      |Michael|
      +-------+
 */

  /**
    * 保存到 MySQL
    * 两种方式
    *
    * ignore 相对于 表 而言
    */
  personDF.write
    .format("jdbc")
    .mode(SaveMode.Ignore)
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("url","jdbc:mysql://127.0.0.1:3306/person")
    .option("user", "root")
    .option("password", "123")
    .option("dbtable", "person")
    .save()

  // 读取文件配置
  val properties = new Properties()
  properties.load(new FileInputStream("/Users/gengmei/IDEA/spark_study/src/main/scala/sql/job.properties"))

  val prop = new Properties
  prop.setProperty("driver", properties.getProperty("driver"))
  prop.setProperty("user", properties.getProperty("user"))
  prop.setProperty("password", properties.getProperty("password"))
  personDF.write.mode("append").jdbc(properties.getProperty("url"), "person", prop)

  // 读取 MySQL 数据
  spark.read
    .format("jdbc")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("url","jdbc:mysql://127.0.0.1:3306/person")
    .option("user", "root")
    .option("password", "123")
    .option("dbtable", "person")
    .load()
    .show()
  /*
    输出：
        +---+-------+
        |age|   name|
        +---+-------+
        | 19|Michael|
        +---+-------+
   */

  /**
    * show(numRows: Int, truncate: Boolean): Unit
    *
    * 默认显示 20 行，是否截断字符串
    */
}
