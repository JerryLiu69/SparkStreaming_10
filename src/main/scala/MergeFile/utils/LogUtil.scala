package MergeFile.utils

import java.text.SimpleDateFormat
import java.util.Date

object LogUtil {
  /*
  * 打印时间日志
  * */
  private val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  def printMessage(message:String): Unit ={
    println("[" + sdf.format(new Date) + "]" + ":" + message)
  }
}
