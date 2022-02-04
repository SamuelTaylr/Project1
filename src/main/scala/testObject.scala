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


  def createDatabase(spark: SparkSession): Unit = {
    //spark.sql("create table bev_branches(beverage String,branch String) row format delimited fields terminated by ','")
    //spark.sql("create table bev_count(beverage String,count Int) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/bev_branches.txt' INTO TABLE bev_branches")
    spark.sql("LOAD DATA LOCAL INPATH 'input/bev_Conscount.txt' INTO TABLE bev_count")
    case class beverages(name: String, branch: String)


    val bevTable = spark.sparkContext.textFile("input/bev_branches.txt")
    val df2 = bevTable.map(_.split(",")).map{case Array(a,b) => (a,b)}.toDF("Beverage","Branch")
    df2.show(100)
    //df2.where("Beverage = Cold_cappuccino").show(10)
    spark.sql("CREATE DATABASE IF NOT EXISTS newTestDB")
    spark.sql("CREATE TABLE IF NOT EXISTS testBevDB(beverage String, branch String) ")
    //df2.write.saveAsTable("newTestDB.testBevDB5")
    spark.sql("SELECT * from newTestDB.testBevDB5")

  }


  def main(args: Array[String]): Unit = {

    menuMethod()

    //spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    //spark.sql("CREATE TABLE IF NOT EXISTS src(key INT, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ‘,’ STORED AS TEXTFILE")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/kv1.txt' INTO TABLE src")
    //spark.sql("CREATE TABLE IF NOT EXISTS src (key INT,value STRING) USING hive")
    //spark.sql("create table newone1(id Int,name String) row format delimited fields terminated by ','");
    //spark.sql("LOAD DATA LOCAL INPATH 'input/kv1.txt' INTO TABLE newone1")
    //spark.sql("SELECT * FROM newone1").show()

  }
}
