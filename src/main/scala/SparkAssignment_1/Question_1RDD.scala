package SparkAssignment_1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Question_1RDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Question1").getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val rddFromFile1 = spark.sparkContext.textFile("src/main/resources/users.csv").collect()
    println(rddFromFile1.getClass)

    val rdd = rddFromFile1.map(f=>{
      f.split(",")
    })

    rdd.foreach(f=>{
      println("Col1:"+f(0)+",Col2:"+f(1))
    })
    println(rdd)

  }
}
