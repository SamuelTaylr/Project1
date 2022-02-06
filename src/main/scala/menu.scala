import org.apache.spark.sql.SparkSession

class menu {

  def selectionMenu(selection: Int, spark: SparkSession) : Unit = {
    val scenario = new scenarios

    selection match {
      case 1 => {
        println("Results for Scenario 1 : ")
        scenario.scenarioOne(spark)
      }
      case 2 => {
        println("Results for Scenario 2 : ")
        scenario.scenarioTwo(spark)
      }
      case 3 => {
        println("Results for Scenario 3 : ")
        scenario.scenarioThree(spark)
      }
      case 4 => {
        println("Results for Scenario 4 : ")
        scenario.scenarioFour(spark)
      }
      case 5 => {
        println("Results for Scenario 5 : ")
        scenario.scenarioFive(spark)
      }
      case 6 => {
        println("Results for Scenario 6 : ")
      }
    }
  }
}
