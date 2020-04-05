package SpakStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/*
* Create by Jerry on 2019/05
* UpdateStateByKey 测试
* */
object SparkStreaming_NET_UpdateStateByKey {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    /*
    * 第一步：获取编程入口
    * */
    val conf = new SparkConf().setAppName("SparkStreaming_NET_UpdateStateByKey").setMaster("local[2]")
    val sparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("WARN")
    val streamingConetxt = new StreamingContext(sparkContext,Seconds(2))
    streamingConetxt.checkpoint("/tmp/tmp/ssc/a")

    /*
    * 第二部：构建DStream
    * */
    val dstream = streamingConetxt.socketTextStream("hadoop03",9999)
    /*
    * 第三部：针对第一个DStream做词频统计操作
    * */
    val wordsDStream = dstream.flatMap(_.split(" ")).map(x => (x,1)).updateStateByKey(updateFunc)

    /*
    * 第四部：处理结果数据，调用 output,Operation
    * */
    wordsDStream.print()

    /*
    * 第五步：启动 SparkStreaming 应用程序，然后等待终结
    * */
    streamingConetxt.start()
    streamingConetxt.awaitTermination()
  }
  def updateFunc(value:Seq[Int],state:Option[Int]) ={
    val newSumValue = value.sum
    val lastValue = state.getOrElse(0)
    Some(newSumValue + lastValue)
  }
}
