package Dftt_Ad_SparkStreaming.utils

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Init {
  def initSpark(appName:String,time:Int): StreamingContext = {
    val conf = new SparkConf().setAppName("dftt_ad_sparkStreaming").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc: StreamingContext = new StreamingContext(sc,Seconds(time))
    ssc
  }
}
