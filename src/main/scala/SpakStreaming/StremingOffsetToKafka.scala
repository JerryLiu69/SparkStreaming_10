package SpakStreaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object StremingOffsetToKafka {
  private val appName = "StreamingTest"
  private val LOG: Logger = LoggerFactory.getLogger(appName)

  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("Usage:StreamingTest <propsName>")
      System.exit(1)
    }
    LOG.info("################ Streaming Start ##################")
    //读取配置文件信息
    val propName = args(0)
    val conf = new SparkConf().setAppName("123").setMaster("local[2]")
    conf.set("spark.streaming.kafka.maxRatePerPartition","200")

    val ssc = new StreamingContext(conf,Seconds(4))

    //kafka参数
    val topics = Set("topic")
    val groupid = "groupid"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "kafka01:9092,kafka02:9092,kafka03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest", // 初次启动从最开始的位置开始消费
      "enable.auto.commit" -> (false: java.lang.Boolean) // 自动提交设置为 false
    )
    // 方法一;使用kafka
    val stream = KafkaUtils.createDirectStream[String,String](
      ssc,
      PreferConsistent,//均匀分发到 executor
      Subscribe[String,String](topics,kafkaParams)
    )
    LOG.info("################## Create Streaming Success ####################")
    stream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition(iter => {
        iter.foreach(line => {
          //处理逻辑
          println(line.value())
          //
        })
      })
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
