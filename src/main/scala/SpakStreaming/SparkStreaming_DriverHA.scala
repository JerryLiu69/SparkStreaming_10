package SpakStreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/*
* Create by Jerry on 2019/05/05
* 保证程序宕掉后，重启不会出错
* */
object SparkStreaming_DriverHA {
  def main(args: Array[String]): Unit = {
    val chkDir = "/tmp/tmp/ssc/b"
    def functionToCreateContext(): StreamingContext = {

      /**
        * 第一步： 获取编程入口
        */
      val sparkConf = new SparkConf().setAppName("SparkStreaming_NET").setMaster("local[2]")
      val sparkContext = new SparkContext(sparkConf)
      sparkContext.setLogLevel("WARN")
      val sc: StreamingContext = new StreamingContext(sparkContext, Seconds(4))
      sc.checkpoint(chkDir)

      /**
        * 第二部： 构建DStream
        */
      val dstream: ReceiverInputDStream[String] = sc.socketTextStream("hadoop03", 9999)

      /**
        * 第三步： 针对第一个DStream做词频统计操作
        */
      val wordsDStream = dstream.flatMap(_.split(" "))
      val wordAndOneDStream: DStream[(String, Int)] = wordsDStream.map(x => (x, 1))

      def updateFunc(value:Seq[Int], state:Option[Int]):Option[Int] = {
        val newSumResult = value.sum
        val lastValue = state.getOrElse(0)
        Some(newSumResult + lastValue)
      }
      val resultDStream = wordAndOneDStream.updateStateByKey(updateFunc).print()

      /**
        * 第四步：  启动SparkStreaming应用程序， 然后等待终结
        */
      sc.start()
      sc.awaitTermination()
      sc
    }
    val context: StreamingContext = StreamingContext.getOrCreate(chkDir, functionToCreateContext )
    context.start()
    context.awaitTermination()
    context.stop()
  }
}
