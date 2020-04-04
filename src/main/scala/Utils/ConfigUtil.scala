package Utils

import com.typesafe.config.{Config, ConfigFactory}

object ConfigUtil {
  def getConf():Config = {
    val conf = ConfigFactory.load("application.conf")
    conf
  }

  def main(args: Array[String]): Unit = {
    val conf = getConf()
    print(conf.getString("kafka.groupid"))
  }
}
