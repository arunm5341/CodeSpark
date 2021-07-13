package SparkAssignment_1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

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

    val stored_data = df1.join(df2,df1("UserID") === df2("UserID"),"inner")






  }
}
