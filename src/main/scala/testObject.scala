import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

object testObject {

  System.setProperty("hadoop.home.dir", "C:\\hadoop\\")

  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  println("created spark session")

  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  def menuMethod(): Unit = {

    val menu = new menu
    println(
      """
        |************************
        |Enter a number to select
        |a menu item.************
        |************************
        |1. Scenario 1***********
        |2. Scenario 2***********
        |3. Scenario 3***********
        |4. Scenario 4***********
        |5. Scenario 5***********
        |6. Unique Scenario******
        |************************
        |""".stripMargin)
    val selection = readInt()
    menu.selectionMenu(selection,spark)

  }

  def main(args: Array[String]): Unit = {

    //createDatabase.createDatabaseFirst(spark)
    menuMethod()

    //spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    //spark.sql("CREATE TABLE IF NOT EXISTS src(key INT, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ‘,’ STORED AS TEXTFILE")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/' INTO TABLE src")
    //spark.sql("CREATE TABLE IF NOT EXISTS src (key INT,value STRING) USING hive")
    //spark.sql("create table newone1(id Int,name String) row format delimited fields terminated by ','");
    //spark.sql("LOAD DATA LOCAL INPATH 'input/' INTO TABLE newone1")
    //spark.sql("SELECT * FROM newone1").show()

  }
}
