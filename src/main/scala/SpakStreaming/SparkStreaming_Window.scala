package SpakStreaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/*
* Create by Jerry on 2019/05
* Window 函数测试
* */
object SparkStreaming_Window {
  def main(args: Array[String]): Unit = {
    /*
    * 获取编程入口
    * */
    val conf = new SparkConf().setAppName("SparkStreaming_Window").setMaster("local")
    val sparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("WARN")
    val streaming = new StreamingContext(sparkContext,Seconds(2))
    /*
    * 构建DStream
    * */
    val dstream = streaming.socketTextStream("hadoop01",9999)
    /*
    * 针对第一个DStream做词频统计操作
    * */
    val wordsDStream = dstream.flatMap(_.split(" ")).map(x => (x,1))
    //没隔 4s 计算过去 6s
    val resultDStream = wordsDStream.reduceByKeyAndWindow((x:Int,y:Int)=> x + y ,Seconds(6),Seconds(4))
    /*
    * 处理结果数据，调用output Operation
    * */
    resultDStream.print()
    /*
    * 启动SparkStreaming应用程序，然后等待终结
    * */
    streaming.start()
    streaming.awaitTermination()
  }
}
