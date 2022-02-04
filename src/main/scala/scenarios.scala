import org.apache.spark.sql.SparkSession

class scenarios {


  def scenarioOne(spark: SparkSession) : Unit = {
    spark.sql("Select sum(c.count) from bev_branch b join bev_count c on b.beverage=c.beverage where b.branch = 'Branch1'").show()
    spark.sql("Select sum(c.count) from bev_branch b join bev_count c on b.beverage=c.beverage where b.branch = 'Branch2'").show()
    spark.sql("select * from bev_branch b join bev_count c on b.beverage = c.beverage").show(100)

  }

  def scenarioTwo(spark: SparkSession) : Unit = {
    //What is the most consumed beverage on Branch1
    //What is the least consumed beverage on Branch2
    //What is the Average consumed beverage of  Branch2
    println("What is the most consumed beverage for Branch 1?")
    spark.sql("select b.beverage, sum(c.count) from bev_branch b join bev_count c on b.beverage = c.beverage where b.branch " +
      "= 'Branch1' group by b.beverage order by sum(c.count) desc limit 1").show()
    println("What is the least consumed beverage for Branch 2?")
    spark.sql("select b.beverage, sum(c.count) from bev_branch b join bev_count c on b.beverage = c.beverage where branch " +
      "= 'Branch2' group by b.beverage order by sum(c.count) asc limit 1").show()
    println("What is the Average consumed beverage of Branch 2?")
    spark.sql("select avg(bev) from (select b.beverage, c.count as bev from bev_branch b join bev_count c on b.beverage = c.beverage where b.branch = 'Branch2' group by b.beverage, c.count)  ").show()

    //spark.sql("select * from (select count(c.beverage) as beverage_count from bev_branch b join bev_count c on b.beverage=c.beverage where b.branch = 'Branch1') where beverage_count = max(beverage_count)").show()

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
