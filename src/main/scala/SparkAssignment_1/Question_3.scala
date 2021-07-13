package SparkAssignment_1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Question_3 {

  // 3) Total spending done by each user on each product.
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Question1").getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    val df1 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/users.csv")

    val df2 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/transactions.csv")

    df2.groupBy("UserID","Product_ID").agg(sum("Price")).show()

  }
}
