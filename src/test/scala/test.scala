import org.apache.spark.sql.SparkSession

object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Structrued_Streaming")
      .master("local[2]")
      .getOrCreate()
    val lines = spark.readStream
      .format("socket")
      .option("host", "192.168.88.100")
      .option("port", 9999).load()
    import spark.implicits._
    val words = lines.as[String].flatMap(_.split(" "))
    val counts = words.groupBy("value").count()
    val query = counts.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    query.awaitTermination()
  }
}
