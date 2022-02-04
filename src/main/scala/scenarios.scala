import org.apache.spark.sql.SparkSession

class scenarios {


  def scenarioOne(spark: SparkSession) : Unit = {
    spark.sql("Select sum(c.count) from bev_branches b join bev_count c on b.beverage=c.beverage where b.branch = 'Branch1'").show()
    spark.sql("Select sum(c.count) from bev_branches b join bev_count c on b.beverage=c.beverage where b.branch = 'Branch2'").show()
    spark.sql("select * from bev_branches b join bev_count c on b.beverage = c.beverage").show(100)

  }

  def scenarioTwo(spark: SparkSession) : Unit = {

  }

  def scenarioThree(spark: SparkSession) : Unit = {

  }

  def scenarioFour(spark: SparkSession) : Unit = {

  }

  def scenarioFive(spark: SparkSession) : Unit = {

  }

  def uniqueScenario(spark: SparkSession) : Unit = {

  }
}
