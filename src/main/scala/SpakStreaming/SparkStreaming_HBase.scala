package SpakStreaming

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import Utils.HbaseUtil._
import Utils.LogUtil._

/*
* Create by Jerry on 2019/05
* Streaming 结果数据保存在HBase
* */
object SparkStreaming_HBase {
  def main(args: Array[String]): Unit = {
    val chkDir = "/tmp/tmp/SparkStreaming_Direct_HBase"
    def functionToCreateContext():StreamingContext ={
      val conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreaming_HBase")
      val sc = new SparkContext(conf)
      sc.setLogLevel("WARN")
      val ssc = new StreamingContext(sc,Seconds(5))
      ssc.checkpoint(chkDir)
      val topics =  Set("flume-kafka")
      val kafkaParams = Map ("metadata.broker.list" -> "hadoop01:9092,hadoop02:9092,hadoop03:9092")


      val lines = KafkaUtils.createDirectStream(
        ssc,
        PreferConsistent, //均匀分发到 executor
        Subscribe[String, String](topics, kafkaParams)
      ).map(_.value()).flatMap(_.split(" ")).map((_,1)).updateStateByKey((x:Seq[Int],y:Option[Int])=>{
        Some(x.sum + y.getOrElse(0))
      }).foreachRDD(rdd => {
        def func(records: Iterator[(String,Int)]): Unit ={
          records.foreach(word => {
            val hbaseConn= getHBaseConn
            val admin = hbaseConn.getAdmin
            val table = hbaseConn.getTable(TableName.valueOf("user"))
            val p = new Put(Bytes.toBytes("69f6db7674a9ae5"))
            //参数:列族,列名,列值
            p.addColumn(Bytes.toBytes("user"),Bytes.toBytes("device"),Bytes.toBytes("69f6db7674a9ae5"))
            p.addColumn(Bytes.toBytes("user"),Bytes.toBytes("qid"),Bytes.toBytes("123"))
            p.addColumn(Bytes.toBytes("user"),Bytes.toBytes("uid"),Bytes.toBytes("10000069"))
            table.put(p)
            printMessage("data insert success ...")
            table.close()
          })
          rdd.foreachPartition(func)
        }
      })
      ssc.start()
      ssc.awaitTermination()
      ssc
    }
    val context = StreamingContext.getOrCreate(chkDir,functionToCreateContext)
    context.start()
    context.awaitTermination()
  }
}
