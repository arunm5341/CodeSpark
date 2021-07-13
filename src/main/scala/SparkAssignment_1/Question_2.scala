package SparkAssignment_1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Question_2 {
  // b)	Find out products bought by each user
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Question1").getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    val df1 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/users.csv")

   // df1.show()

    val df2 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/transactions.csv")

   // df2.show()

    // df2.dropDuplicates("UserId","Product_ID").show()

    df2.select("Product_ID","UserID").show()


  }
}