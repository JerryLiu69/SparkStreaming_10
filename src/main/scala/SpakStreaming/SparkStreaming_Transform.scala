package SpakStreaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/*
* Create by Jerry on 2019/05
* Transform 测试
* */
object SparkStreaming_Transform {
  def main(args: Array[String]): Unit = {
    /*
    * 第一步：获取编程入口
    * */
    val conf = new SparkConf().setAppName("SparkStreaming_Transform").setMaster("local")
    val sparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("WARN")
    val sparkStreaming = new StreamingContext(sparkContext,Seconds(4))
    /*
    * 第二部：构建DStream
    * */
    val dstream = sparkStreaming.socketTextStream("hadoop03",9999)
    /*
    * 第三部：对特殊符号进行过滤
    * */
    val list = List(",",".","?","!","#","@","$","_")
    val wordsDStream = dstream.flatMap(_.split(" "))

    val transformFunc = (rdd:RDD[String]) =>{
      // 大括号里的代码在 execution 中执行
      val diffRDD = rdd.subtract(sparkContext.makeRDD(list))
      diffRDD
    }

    val wordFilterDStream = wordsDStream.transform(transformFunc).map(x => (x,1)).reduceByKey(_ + _).print()
    /*
    * 第四部: 启动SparkStreaming 应用程序，然后等待终结
    * */
    sparkStreaming.start()
    sparkStreaming.awaitTermination()
  }
}
