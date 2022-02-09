import org.apache.spark.sql.SparkSession

class scenarios {


  def scenarioOne(spark: SparkSession) : Unit = {
    /*Problem Scenario 1
    What is the total number of consumers for Branch1?
      What is the number of consumers for the Branch2?*/
    println("What is the total number of consumers for Branch 1?")
    spark.sql("Select sum(c.count) from bev_branch b join bev_count c on b.beverage=c.beverage where b.branch = 'Branch1'").show()
    println("What is the total number of consumers for Branch 2?")
    spark.sql("Select sum(c.count) from bev_branch b join bev_count c on b.beverage=c.beverage where b.branch = 'Branch2'").show()
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
    val df = spark.sql("select b.beverage, sum(c.count) from bev_branch b join bev_count c on b.beverage = c.beverage where branch " +
      "= 'Branch2' group by b.beverage order by sum(c.count)")
    val df2 = df.take(26).last.toString()
    println("The average consumed beverage at Branch 2 is :" + df2)

    /*spark.sql("select avg(bev) as Avg_Num_Consumptions_Per_Beverage from (select b.beverage, c.count as bev from " +
      "bev_branch b join bev_count c on b.beverage = c.beverage where b.branch = 'Branch2' group by b.beverage, c.count)  ").show()
    spark.sql("Select avg(cf) from (Select b.beverage, sum(c.count) as cf from bev_branch b join bev_count c on " +
      "b.beverage=c.beverage where b.branch='Branch2' group by b.beverage) as counts").show()*/
    //spark.sql("select * from (select count(c.beverage) as beverage_count from bev_branch b join bev_count c on
    // b.beverage=c.beverage where b.branch = 'Branch1') where beverage_count = max(beverage_count)").show()
  }

  def scenarioThree(spark: SparkSession) : Unit = {
    /*Problem Scenario 3
    What are the beverages available on Branch10, Branch8, and Branch1?
    what are the comman beverages available in Branch4,Branch7?*/
    println("What are the beverages available in Branch 10, Branch 8 and Branch 1?")
    println("Listed Separately: ")
    spark.sql("select distinct beverage as Unique_Beverages_Branch8 from bev_branch where branch = 'Branch8'").show()
    spark.sql("select distinct beverage as Unique_Beverages_Branch1 from bev_branch where branch = 'Branch1'").show()
    println("Listed together: ")
    spark.sql("select * from (select distinct beverage as Unique_Beverages_Branch8_And_Branch1 from bev_branch where branch = 'Branch8' INTERSECT " +
      "select distinct beverage as Unique_Beverages_Branch8_And_Branch1 from bev_branch where branch = 'Branch1')").show()

  }

  def scenarioFour(spark: SparkSession) : Unit = {
    /*Problem Scenario 4
    create a partition,View for the scenario3.*/
    println("Showing a grouped select query on a table partitioned by Branch: ")
    spark.sql("create table if not exists Branch_Partition (Beverage String) partitioned by(Branch String) row format delimited fields terminated by ','")
    //spark.sql("insert overwrite table Branch_Partition partition(Branch = 'Branch1') select distinct beverage from bev_branch where branch = 'Branch1'")
    //spark.sql("insert overwrite table Branch_Partition partition(Branch = 'Branch8') select distinct beverage from bev_branch where branch = 'Branch8'")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt' INTO TABLE Branch_Partition partition(Branch=Branch1)")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt' INTO TABLE Branch_Partition partition(Branch=Branch8)")
    spark.sql("describe formatted Branch_Partition").show()
    spark.sql("show partitions Branch_Partition").show()
    spark.sql("select distinct beverage, branch from Branch_Partition group by branch, beverage order by branch ").show(100)
    println("Showing a new view created with the results of select queries from scenario 3: ")
    spark.sql("create view if not exists Unique_Beverages as select distinct beverage, branch from bev_branch where branch = 'Branch1' or branch = 'Branch8' ")
    spark.sql("select distinct beverage, branch from Unique_Beverages group by branch, beverage order by branch").show(100)
  }

  def scenarioFive(spark: SparkSession) : Unit = {
    /*Problem Scenario 5
    Alter the table properties to add "note","comment"
    Remove a row from the any Senario.*/
    println("Alter a table to add comments")
    //spark.sql("drop table scenario5_table")
    spark.sql("create table if not exists scenario5_table(Beverage String comment 'Beverage Name', Branch String " +
      "comment 'Branch Number') row format delimited fields terminated by ','")
    //spark.sql("load data local inpath 'input/Bev_Branch.txt' into table scenario5_table")
    //spark.sql(s"ALTER TABLE scenario5_table SET TBLPROPERTIES('notes' = 'This table has a note.')")
    spark.sql("describe formatted scenario5_table").show()
    spark.sql("SHOW TBLPROPERTIES scenario5_table").show()

    println("Remove a row from any scenario")
    println("I decided to remove Branch1 from a table")
    //spark.sql("drop table scenario5_table_temp")
    spark.sql("create table if not exists scenario5_table_temp(Beverage String comment 'Beverage Name', Branch " +
      "String) row format delimited fields terminated by ','")
    //spark.sql("insert into scenario5_table_temp (select Beverage, Branch from scenario5_table where branch NOT LIKE 'Branch1')")
    //spark.sql("delete from scenario5_table_temp1 where branch = 'Branch1'")
    //spark.sql("alter table scenario5_table replace columns(Beverage String comment 'Beverage Name')")
    spark.sql("select * from scenario5_table_temp where branch = 'Branch1'").show()
    spark.sql("select * from scenario5_table_temp order by branch").show()

  }

  def uniqueScenario(spark: SparkSession) : Unit = {
    //Future query, insert price data for each drink type, then join on bev_count table and find which Branch has
    // the highest gross sales and which drinks bring the most money to that branch.
    spark.sql("create table if not exists bev_price6(Beverage String comment 'Beverage Name',Price Double comment 'Beverage Price') row format delimited fields terminated by ','")
    //spark.sql("drop table bev_branch6_withPrice")
    spark.sql("create table if not exists bev_branch6(Beverage String comment 'Beverage Name',Branch String comment 'Branch Number') row format delimited fields terminated by ','")
    spark.sql("create table if not exists bev_count6(Beverage String,Count Int) row format delimited fields terminated by ','")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt' INTO TABLE bev_branch6")
    //spark.sql("load data local inpath 'input/Bev_Prices.txt' into table bev_price6")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Conscount.txt' INTO TABLE bev_count6")
    // This method wont work, need to create a separate price table with each beverage and join it to this table
    //spark.sql("insert into bev_branch6_withPrice(Price) values(3.35) where Beverage = 'SMALL_Espresso'")
    //spark.sql("select * from bev_price6 b join bev_branch6 c on b.beverage=c.beverage join bev_count6 d on c.beverage=d.beverage").show(100)
    //spark.sql("select sum(b.price * d.count) as Profits_Sml_Espresso_Branch_2 from bev_price6 b join bev_branch6 c on b.beverage=c.beverage join bev_count6 d on c.beverage=d.beverage where c.branch = 'Branch2' and c.beverage= 'SMALL_Espresso'").show(100)
    //spark.sql("select * from bev_price6 b join bev_branch6 c on b.beverage=c.beverage join bev_count6 d on c.beverage=d.beverage").show(100)
    spark.sql("select sum(price*count) as Branch1_Profits from (select * from bev_price6 b join bev_branch6 c on b.beverage=c.beverage join bev_count6 d on c.beverage=d.beverage) where branch='Branch1'").show()
  }
}
