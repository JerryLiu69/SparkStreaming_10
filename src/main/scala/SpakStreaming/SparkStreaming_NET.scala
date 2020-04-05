package SpakStreaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/*
* Create by Jerry on 2019/05
* 实时监控端口，测试用
* */
object SparkStreaming_NET {
  def main(args: Array[String]): Unit = {
    /*
    * 获取编程入口
    * */
    val conf = new SparkConf().setAppName("sparkStream_NET").setMaster("local")
    val sparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("WARN")
    val streamingContext = new StreamingContext(sparkContext,Seconds(4))
    /*
    * 构建DStrea
    * */
    val DStream = streamingContext.socketTextStream("hadoop01",9999)
    /*
    * 第三步： 针对第一个DStream做词频统计操作
    * */
    val wordsDStream = DStream.flatMap(_.split(" "))
    val wordAndOneDStream = wordsDStream.map(x => (x,1))
    val resultDStream = wordAndOneDStream.reduceByKey(_ + _)
    /*
    * 处理结果数据，调用 output，Operation
    * */
    resultDStream.print()
    /*
    * 启动SparkStreaming 应用程序，然后等待终结
    * */
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
