package Utils

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

object HbaseUtil {
  private val config = ConfigFactory.load()
  private val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", config.getString("hbase.zkHosts"))
  conf.set("hbase.zookeeper.property.clientPort", config.getString("hbase.zkPort"))
  private val connection: Connection = ConnectionFactory.createConnection(conf)
  def getHBaseConn = {
    connection
  }
}
