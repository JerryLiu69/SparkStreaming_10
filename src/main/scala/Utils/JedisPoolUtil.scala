package Utils

import com.typesafe.config.{Config, ConfigFactory}
import redis.clients.jedis.{Jedis,JedisPool, JedisPoolConfig}

object JedisPoolUtil {
  //加载配置文件
  private val config: Config = ConfigFactory.load()
  private val host: String = config.getString("redis.host")
  private val auth: String = config.getString("redis.auth")
  private val port: Int = config.getInt("redis.port")
  private val jedisConfig = new JedisPoolConfig
  //最大连接数
  jedisConfig.setMaxTotal(config.getInt("redis.maxConn"))
  //最大空闲连接数
  jedisConfig.setMaxIdle(config.getInt("redis.maxIdle"))
  //设置连接池属性
  val pool = new JedisPool(jedisConfig,host,port,10000,auth)
  def getConnections():Jedis = {
    pool.getResource
  }
}
