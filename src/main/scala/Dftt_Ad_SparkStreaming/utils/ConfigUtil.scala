package Dftt_Ad_SparkStreaming.utils

import com.typesafe.config.{Config, ConfigFactory}

object ConfigUtil {
  def getConf():Config = {
    val conf = ConfigFactory.load("application.conf")
    conf
  }
}
