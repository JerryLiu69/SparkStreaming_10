package Utils

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingUtil {
  def initStreamingContext(appName:String,seconds:Int): StreamingContext ={
    val conf = new SparkConf().setMaster("local[2]").setAppName(appName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc,Seconds(seconds))
    ssc
  }
  def getConf(): Config ={
    val config = ConfigFactory.load()
    config
  }
}
