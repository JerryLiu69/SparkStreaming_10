package SpakStreaming

import Utils.JedisPoolUtil
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010._
import scala.collection.mutable
import scala.util.Try

object StreamingOffsetToRedis {
  def getOffset(topics:Set[String],groupid:String): mutable.Map[TopicPartition,Long] ={
    val fromOffset: mutable.Map[TopicPartition, Long] = scala.collection.mutable.Map[TopicPartition,Long]()
    //获取 redis 中存的值
    val jedis = JedisPoolUtil.getConnections()
    topics.foreach(topic => {
      val keys = jedis.keys(s"offset_${groupid}_${topic}")
      if(!keys.isEmpty){
        keys.forEach(key =>{
          val offset = jedis.get(key)
          val partition = Try(key.split(s"offset_${groupid}_${topic}_").apply(1)).getOrElse("0")
          //输出
          println(partition + "::" + offset)
          fromOffset.put(new TopicPartition(topic,partition.toInt),offset.toLong)
        })
      }
    })
    jedis.close()
    fromOffset
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingOffsetToRedis").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc,Seconds(2))
    //kafka topic
    val topics = Set("offset-redis-01")
    //kafka params
    val kafkaParams: Map[String, Object] = Map(
      "bootstrap.servers" -> "node1:9092,node2:9092,node3:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "offSet-Redis-Test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val groupid = kafkaParams.get("group.id").get.toString
    //获取消费方式消费方式有三种
    //earlist:当个分区下有已经提交的offset时候，从提交的offset开始消费，无提交的offset时，从头开始消费
    //latest：当各分区下有已经提交的offset时候，从提交的offset开始消费，无提交的offset时，消费新产生的该分区下的数据
    //none:topic各分区都存在已经提交的offset时，从offset后开始消费，只要有一个分区不存在已提交的offset，则抛出异常
    val reset = kafkaParams.get("auto.offset.reset").get.toString
    //获取偏移量
    val offsets = getOffset(topics,groupid)
    //spark读取分方式，均匀分布
    val locationStrategy: LocationStrategy = LocationStrategies.PreferConsistent
    val conumerStrategy: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe(topics,kafkaParams,offsets)

    val kafkaInputStream = KafkaUtils.createDirectStream(ssc,locationStrategy,conumerStrategy)
    kafkaInputStream.foreachRDD(rdd =>{
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      if(!rdd.isEmpty()){
        val jedis = JedisPoolUtil.getConnections()
        //开启jedis事务
        val transaction = jedis.multi()
        //代码逻辑
        rdd.foreachPartition(result =>{
          reset.foreach(println)
        })
        //代码逻辑
        offsetRanges.foreach(iter => {
          val key = s"offset_${groupid}_${iter.topic}_${iter.partition}"
          val value = iter.untilOffset
          transaction.set(key,value.toString)
        })
        transaction.exec()
        transaction.clone()
        jedis.close()
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
