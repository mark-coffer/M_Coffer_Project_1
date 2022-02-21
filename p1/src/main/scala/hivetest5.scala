import org.apache.spark.sql.SparkSession

import scala.language.postfixOps
import scala.io.StdIn

object hivetest5 {

  def switchIntro: Char = {
    println("")
    println("PLEASE SELECT A SCENARIO")
    println("(Available options are: 1, 2, 3, 4, 5, 6. Press 'x' to QUIT)")
    print("Selection: ")
    return StdIn.readChar()
  }

  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    // This comment block is used for creation of the necessary information - mac
    /*
    spark.sql("Drop table bevCountAll")
    spark.sql("drop table bevBranchA")
    spark.sql("drop table bevBranchB")
    spark.sql("drop table bevBranchC")
    spark.sql("drop table bevCountA")   -Used during templating process - mac
    spark.sql("drop table bevCountB")
    spark.sql("drop table bevCountC")

    //--Create All Appropriate Tables
    spark.sql("CREATE TABLE IF NOT EXISTS bevCountAll(beverage String,saleNum int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConsCount_Combine.txt' INTO TABLE bevCountAll")
    spark.sql("SELECT * FROM bevCountAll").show()

    spark.sql("CREATE TABLE IF NOT EXISTS bevBranchA(beverage String,branch_id String) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE bevBranchA")
    spark.sql("SELECT * FROM bevBranchA").show()

    spark.sql("CREATE TABLE IF NOT EXISTS bevBranchB(beverage String,branch_id String) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE bevBranchB")
    spark.sql("SELECT * FROM bevBranchB").show()

    spark.sql("CREATE TABLE IF NOT EXISTS bevBranchC(beverage String,branch_id String) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE bevBranchC")
    spark.sql("SELECT * FROM bevBranchC").show()

    spark.sql("CREATE TABLE IF NOT EXISTS bevCountA(beverage String,saleNum int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' INTO TABLE bevCountA")
    spark.sql("SELECT * FROM bevCountA").show()

    spark.sql("CREATE TABLE IF NOT EXISTS bevCountB(beverage String,saleNum int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' INTO TABLE bevCountB")
    spark.sql("SELECT * FROM bevCountB").show()

    spark.sql("CREATE TABLE IF NOT EXISTS bevCountC(beverage String,saleNum int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' INTO TABLE bevCountC")
    spark.sql("SELECT * FROM bevCountC").show()

    Scenario4 creation
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("CREATE TABLE IF NOT EXISTS Scenario4(beverage String) PARTITIONED BY(branch_id String)")
    spark.sql("INSERT OVERWRITE TABLE Scenario4 PARTITION(branch_id) SELECT beverage,branch_id FROM bevBranchC")
    spark.sql("INSERT INTO Scenario4 PARTITION(branch_id) SELECT beverage,branch_id FROM bevBranchB")

    Scenario5 creation
    spark.sql("CREATE TABLE IF NOT EXISTS Scenario5(beverage String, branch_id String)")
    */
    var i = switchIntro
    var presentationActive = true
    while(presentationActive) {
      i match {
        case '1' => {
          println("Scenario 1")
          println("Total number of consumers for Branch 1")
          spark.sql("SELECT SUM(saleNum) FROM bevCountA").show()
          println("")
          println("Total number of consumers for Branch 2")
          spark.sql("SELECT SUM(saleNum) FROM ((SELECT saleNum FROM bevCountA) UNION ALL (SELECT saleNum FROM bevCountC))").show()
          i = switchIntro
        }
        case '2' => {
          println("Scenario 2")
          println("Most consumed beverage at Branch 1")
          spark.sql("SELECT beverage, SUM(saleNum) AS conSum FROM bevCountA GROUP BY beverage ORDER BY conSum DESC LIMIT 1").show()
          println("")
          println("Least consumed beverage at Branch 2")
          spark.sql("SELECT beverage, SUM(saleNum) AS conSum FROM ((SELECT beverage, saleNum FROM bevCountA) UNION ALL (SELECT beverage, saleNum FROM bevCountC)) GROUP BY beverage ORDER BY conSum ASC LIMIT 1").show()
          print("")
          println("Average number of consumed beverages at Branch 2")
          spark.sql("SELECT AVG(saleNum) FROM ((SELECT saleNum FROM bevCountA) UNION ALL (SELECT saleNum FROM bevCountC))").show()
          i = switchIntro
        }
        case '3' => {
          println("Scenario 3")
          println("What beverages are available at Branch 10, 8, 1?")
          println("**Branch 10 does not exist based on the data given**")
          println("Branch 8: ")
          spark.sql("SELECT DISTINCT beverage FROM bevBranchB WHERE branch_id = 'Branch8'").show()
          println("Branch 1: ")
          spark.sql("SELECT DISTINCT beverage FROM bevBranchA WHERE branch_id = 'Branch1'").show()
          println("")
          println("common beverages from branch 4 and 7")
          spark.sql("SELECT DISTINCT beverage FROM ((SELECT beverage FROM bevBranchB WHERE branch_id='Branch7') UNION ALL (SELECT beverage FROM bevBranchC WHERE branch_id='Branch4' OR branch_id='Branch7'))").show()
          i = switchIntro
        }
        case '4' => {
          println("Scenario 4")
          println("Partition Scenario 3")
          spark.sql("describe formatted Scenario4").show()
          spark.sql("SELECT * FROM Scenario4").show()
          i = switchIntro
        }
        case '5' => {
          println("Scenario 5")
          println("Add a note,comment")
          //spark.sql("ALTER TABLE Scenario4 SET TBLPROPERTIES('note' = 'This is a comment')") // --already called as part of demo creation --mac
          spark.sql("SHOW TBLPROPERTIES Scenario4").show()
          spark.sql("SELECT COUNT(branch_id) FROM Scenario4").show()
          println("Remove a row")
          //spark.sql("INSERT INTO Scenario5 SELECT beverage, branch_id FROM Scenario4 LIMIT 499")
          spark.sql("SELECT COUNT(branch_id) FROM Scenario5").show()
          i = switchIntro
        }
        case '6' => {
          println("Scenario 6")
          println("Future Query -- General Menu Options for all locations based on sales")
          println("Taking TOP 10 RESULTS")
          spark.sql("SELECT beverage, SUM(saleNum) AS conSum FROM bevCountAll GROUP BY beverage ORDER BY conSum DESC").show()

          println("""based on the results we will select the first 8 results with the highest saleNums among all queries
            those beverages are:
            1) Special_cappuccino
            2) Mild_cappuccino
            3) LARGE_cappuccino -I am assuming this one is broken down into sizes and wont be including med or small
            4) ICY_cappuccino
            5) Double_cappuccino
            6) Triple_cappuccino
            7) Cold_cappuccino
            8) DOUBLE_LATTE
            9) DOUBLE_Espresso
            10) SPECIAL_Coffee
          """)
          i = switchIntro
        }
        case 'x' => {println("Good Bye"); presentationActive = false}
        case 'X' => {println("Good Bye"); presentationActive = false}
        case _ => {
          println("Please Select a Valid Option"); i = switchIntro
        }
      }
    }
  }
}