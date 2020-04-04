package Dftt_Ad_SparkStreaming.utils

import java.text.SimpleDateFormat
import java.util.Date

object LogUtil {
  private val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  def printMessage(message:String): Unit ={
    println("[" + sdf.format(new Date) + "]" + ":" + message)
  }
}
