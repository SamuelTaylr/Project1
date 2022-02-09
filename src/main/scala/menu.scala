import org.apache.spark.sql.SparkSession

class menu {

  def selectionMenu(spark: SparkSession) : Unit = {
    val scenario = new scenarios
    println(
      """
        |************************
        |Enter a number to select
        |a menu item.************
        |************************
        |1. Scenario 1 **********
        |2. Scenario 2 **********
        |3. Scenario 3 **********
        |4. Scenario 4 **********
        |5. Scenario 5 **********
        |6. Unique Scenario *****
        |7. Quit ****************
        |************************
        |""".stripMargin)
    val selection = readInt()

    selection match {
      case 1 => {
        println("Results for Scenario 1 : ")
        scenario.scenarioOne(spark)
        selectionMenu(spark)
      }
      case 2 => {
        println("Results for Scenario 2 : ")
        scenario.scenarioTwo(spark)
        selectionMenu(spark)
      }
      case 3 => {
        println("Results for Scenario 3 : ")
        scenario.scenarioThree(spark)
        selectionMenu(spark)
      }
      case 4 => {
        println("Results for Scenario 4 : ")
        scenario.scenarioFour(spark)
        selectionMenu(spark)
      }
      case 5 => {
        println("Results for Scenario 5 : ")
        scenario.scenarioFive(spark)
        selectionMenu(spark)
      }
      case 6 => {
        println("Results for Scenario 6 : ")
        scenario.uniqueScenario(spark)
        selectionMenu(spark)
      }
      case 7 => {
        sys.exit(0)
      }
    }
  }
}
