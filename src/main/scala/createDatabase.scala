import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import testObject.spark

object createDatabase {

  import spark.implicits._

  def createDatabaseFirst(spark: SparkSession): Unit = {
    spark.sql("create table if not exists bev_branch(Beverage String,Branch String) row format delimited fields terminated by ','")
    spark.sql("create table if not exists bev_count(Beverage String,Count Int) row format delimited fields terminated by ','")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt' INTO TABLE bev_branch")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Conscount.txt' INTO TABLE bev_count")
    //case class beverages(name: String, branch: String)
    //val bevTable = spark.sparkContext.textFile("input/bev_branch.txt")
    //val df2 = bevTable.map(_.split(",")).map{case Array(a,b) => (a,b)}.toDF("Beverage","Branch")
    //df2.show(100)
    //df2.where("Beverage = Cold_cappuccino").show(10)
    //spark.sql("CREATE DATABASE IF NOT EXISTS newTestDB")
    //spark.sql("CREATE TABLE IF NOT EXISTS testBevDB(beverage String, branch String) ")
    //df2.write.saveAsTable("newTestDB.testBevDB5")
    //spark.sql("SELECT * from newTestDB.testBevDB5")

  }

}
