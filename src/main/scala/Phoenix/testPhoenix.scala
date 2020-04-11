package Phoenix

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object testPhoenix {
  def main(args: Array[String]): Unit = {
    val jdbcPhoenixUrl = "jdbc:phoenix:hadoop01,hadoop02,hadoop03:2181"
    val tableName = "user_info"
    val conf = new SparkConf().setAppName("SparkPhoenix").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)
    val df = sql.load("org.apache.phoenix.spark",Map("table" -> tableName, "zkUrl" -> jdbcPhoenixUrl))
    df.show()

    sc.stop()
  }
}
