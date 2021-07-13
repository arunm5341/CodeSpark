package SparkAssignment_1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}


object Question_1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Question1").getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val df1= spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("src/main/resources/users.csv")

    //df1.show()

    val df2 =spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("src/main/resources/transactions.csv")

    //df2.show()

    val stored_data=df1.join(df2,df1("UserID") === df2("UserID"),"inner")

    stored_data.groupBy("Location").count().show()

    //stored_data.select(col("Location"),count("Product_ID")).show()

  }
}
