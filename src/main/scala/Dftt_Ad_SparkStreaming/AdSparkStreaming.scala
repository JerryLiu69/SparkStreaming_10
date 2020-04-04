package Dftt_Ad_SparkStreaming

import Dftt_Ad_SparkStreaming.utils.LogUtil._
import Dftt_Ad_SparkStreaming.utils._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.slf4j.{Logger, LoggerFactory}
import Dftt_Ad_SparkStreaming.deploy.log_process._

/*
* Create by Jerry on 2019/06/10
* */
object AdSparkStreaming {
  def data_from_kafka()={
    val ssc = Init.initSpark("Ad_SparkStreaming", 5)
    val config = ConfigUtil.getConf()
    val groupid = config.getString("kafka.groupid")
    val brokerList = config.getString("kafka.brokerList")
    val topic = config.getString("topic")
    val topics = Set("topic")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokerList,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest", // 初次启动从最开始的位置开始消费
      "enable.auto.commit" -> (false: java.lang.Boolean) // 自动提交设置为 false
    )
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      PreferConsistent, //均匀分发到 executor
      Subscribe[String, String](topics, kafkaParams)
    )
    (stream,ssc)
  }

  /*
  * Ad 广告处理逻辑主方法
  * */
  def main(args: Array[String]): Unit = {
    val appName = "AdSparkStreaming"
    val LOG: Logger = LoggerFactory.getLogger(appName)
    if(args.length != 1){
      printMessage("Usage:StreamingTest <propsName>")
      System.exit(1)
    }
    LOG.info("################ Streaming Start ##################")
    val ssc = data_from_kafka()._2
    val stream = data_from_kafka()._1

    stream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //处理逻辑
      evaluateZbbLog(rdd.map(_.value()))

      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
