package Dftt_Ad_SparkStreaming.deploy

import java.util
import Dftt_Ad_SparkStreaming.LogInfo
import Dftt_Ad_SparkStreaming.utils.{JedisPoolUtil, TimeUtil}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD

/*
* Create by Jerry on 2019/06/10
* */
object log_process {
  def analytic_logInfo(lines:String):LogInfo={
    try{
      val json_object: JSONObject = JSON.parseObject(lines)
      val id = json_object.getString("id")
      val ad_id = json_object.getString("ad_id")
      val user_id = json_object.getString("user_id")
      val qid = json_object.getString("qid")
      val os = json_object.getString("os")
      val app_version = json_object.getString("app_version")
      val ime = json_object.getString("ime")
      val time = json_object.getLongValue("time")
      LogInfo(id,ad_id,user_id,qid,os,app_version,ime,time)
    } catch {
      case e: Exception => LogInfo("", "", "", "", "", "","",0L)
    }
  }
  def evaluateZbbLog(rdd: RDD[String]): Unit ={
    //数据逻辑处理
    // 从数据库连接池中获取连接 redis
    val jedis = JedisPoolUtil.getConnections()
    try{
      rdd.map(record => analytic_logInfo(record)).filter(_.id.equals("zbb")).filter(_.time.toString.length == 13)
        .foreachPartition(lines => {
          lines.foreach(line => {
            //拼接key样式 时间_设备号
            val day = TimeUtil.getNowDay().toString
            val yesDay = TimeUtil.getYesDay(line.time).toString
            val id = line.id
            val key = day + "_" + id
            val hiskey = "his_" + id
            val oldkey = "seven_" + id
            /*
            * 在线用户
            * */
            if(jedis.exists(key) == false){
              // 活跃用户数 +1
              jedis.hincrBy("zbb_active",day,1)
              val a: String = jedis.hget("zbb_active",day)
              val b: String = jedis.hget("zbb_active",yesDay)
              //在线环比
              jedis.hset("zbb_active",day+"_percent",((a.toDouble - b.toDouble) / a.toDouble).formatted("%.2f"))
            }
            jedis.set(key,"1")
            jedis.expire(key,60 * 60 * 30)
            /*
            * 新增用户
            * */
            if(jedis.exists(hiskey) == false){
              //新增用户数 + 1
              jedis.hincrBy("zbb_add",day,1)
              val a = jedis.hget("zbb_add",day)
              val b = jedis.hget("zbb_add",yesDay)
              //新增环比
              jedis.hset("zbb_add",day+"_percent",((a.toDouble - b.toDouble) / a.toDouble).formatted("%.2f"))
            }
            jedis.set(hiskey,"1")
            /*
            * 次留
            * */
            if(jedis.exists(yesDay) == true){
              jedis.hincrBy("zbb_second",day,1)
              val a = jedis.hget("zbb_second",day)
              val b = jedis.hget("zbb_second",yesDay)
              //次留环比
              jedis.hset("zbb_second",day+"_percent",((a.toDouble-b.toDouble)/a.toDouble).formatted("%.2f"))
            }
            /*
            * 七留
            * */
            val result: util.Set[String] = jedis.sinter(yesDay, (yesDay.toInt - 1).toString,
              (yesDay.toInt - 1).toString,
              (yesDay.toInt - 1).toString,
              (yesDay.toInt - 1).toString,
              (yesDay.toInt - 1).toString,
              (yesDay.toInt - 1).toString,
              (yesDay.toInt - 1).toString)
            if(result.contains(oldkey)){
              jedis.hincrBy("zbb_seven",day,1)
              //七留环比
              val a = jedis.hget("zbb_seven",day)
              val b = jedis.hget("zbb_seven",yesDay)
              jedis.hset("zbb_seven",day+"_seven",((a.toDouble-b.toDouble)/a.toDouble).formatted("%.2f"))
            }
          })
          jedis.close()
        })
    }catch {
      case e: Exception => println(e.getStackTrace)
    } finally {
      //关闭数据库连接
      jedis.close()
    }
  }
}
