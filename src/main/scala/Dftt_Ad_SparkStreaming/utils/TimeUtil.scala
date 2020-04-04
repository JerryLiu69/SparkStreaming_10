package Dftt_Ad_SparkStreaming.utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

object TimeUtil {
  /* 小时：yyyyMMddHH */
  def getHour(longTime:Long):String = {
    val dateFormat = FastDateFormat.getInstance("yyyyMMddHH")
    var strTime: String = ""
    if(longTime.toString.length ==10){
      strTime = dateFormat.format((longTime + "000").toLong)
    } else {
      strTime = dateFormat.format(longTime)
    }
    strTime
  }
  /* 今天：yyyyMMdd */
  def getNowDay():String = {
    val dateFormat = FastDateFormat.getInstance("yyyyMMdd")
    val strTime = dateFormat.format(new Date())
    strTime
  }
  /* 昨天：yyyyMMdd */
  def getYesDay(longTime:Long):String = {
    val dateFormat = FastDateFormat.getInstance("yyyyMMdd")
    var strTime = ""
    if(longTime.toString.length == 10){
      strTime = dateFormat.format((longTime + "000").toLong - 86400000)
    }else{
      strTime = dateFormat.format(longTime - 86400000)
    }
    strTime
  }
  /* 今天：yyyy-MM-dd */
  def get_Now_Day(str:Long):String = {
    val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd")
    var strTime = dateFormat.format(str)
    if(str.toString.length == 10){
      strTime = dateFormat.format((str + "000").toLong)
    }else{
      strTime = dateFormat.format(str)
    }
    strTime
  }

  def main(args: Array[String]): Unit = {

    print("ok")

  }
}
