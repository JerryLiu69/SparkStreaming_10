package Redis

import java.util
import Utils.JedisPoolUtil._

/*
* Create By Jerry on 2019/05/23
* 以下用的不多，我一般用作 SparkStreaming 日活，留存计算结果数据保存库，操作方面，相应实时。Offset 也可以维护在Redis中。
* */
object RedisApi {
  def main(args: Array[String]): Unit = {
    val jedis = getConnections()

    /* 字符串，整数 */
    //清除数据
    jedis.flushDB()
    //判断某个在是否存在
    jedis.exists("hello")
    //新增
    jedis.set("hello","tom")
    //添加
    jedis.get("hello")
    //删除
    jedis.del("hello")
    //获取系统中的所有键
    jedis.keys("*").toString
    //设置键 a 的过期时间为5s
    jedis.expire("a",5)
    //获取键 a 的剩余生存时间
    jedis.ttl("a")
    //移除键 a 的生存时间
    jedis.persist("a")
    //查看键 a 存的值的数据类型
    jedis.`type`("a")
    //新增键值对防止覆盖原先值
    jedis.setnx("Hello","Jerry")
    //新增键值对并设置有效时间
    jedis.setex("a",5,"b")
    //获取原值，更新为新值
    jedis.getSet("Hello","Tom")
    //截取key002的值的字符串
    jedis.getrange("Hello",2,5)


    /* 整数和浮点型 */
    //将键key001的值+1
    jedis.incr("a")
    //将键key002的值-1
    jedis.decr("a")
    //将 key 的值加上整数 10
    jedis.incrBy("key",10)
    //将 key 的值加上整数 10
    jedis.decrBy("key",10)

    /* List */
    //Redis 列表是简单的字符串列表，按照插入顺序排序。
    // 你可以添加一个元素到列表的头部（左边）或者尾部（右边）。
    jedis.lpush("collections", "ArrayList", "LinkedList", "Vector", "Stack", "queue")
    //collections 的内容
    jedis.lrange("collections",0,-1)
    //collections区间0-2内容
    jedis.lrange("collections",0,2)
    //删除指定元素个数
    jedis.lrem("collections",2,"queue")
    //删除区间0-4以外的数据
    jedis.ltrim("collections",0,4)
    //collections列表出栈（左端）
    jedis.lpop("collections")
    //collections添加元素，从列表右端，与lpush相对应
    jedis.rpush("collections","ArrayList")
    //collections列表出栈（右端）
    jedis.rpop("collections")
    //修改collections指定下标1的内容
    jedis.lset("collections",2,"aaa")
    //collections的长度
    jedis.llen("collections")
    //获取collections下标为2的元素
    jedis.lindex("collection",2)
    //排序
    jedis.lpush("sortedList", "3", "6", "2", "0", "7", "4")
    println("sortedList排序前：" + jedis.lrange("sortedList", 0, -1))
    println(jedis.sort("sortedList"))
    println("sortedList排序后：" + jedis.lrange("sortedList", 0, -1))


    /* set(集合) */
    //Redis的Set是string类型的无序集合。
    //集合是通过哈希表实现的，所以添加，删除，查找的复杂度都是O(1)。
    //"集合set添加数据：
    jedis.sadd("setElement", "e1", "e7", "e3", "e6", "e0", "e4")
    //setElement的所有元素
    jedis.smembers("setElement")
    //删除元素e0:
    jedis.srem("setElement","e0")
    //删除两个元素e7和e6
    jedis.srem("setElement","e1","e3")
    //随机的移除集合中的一个元素
    jedis.spop("setElement")
    //setElement中包含元素的个数
    jedis.scard("setElement")
    //e3是否在setElement中
    jedis.sismember("setElement","e3")


    jedis.sadd("aa", "e1", "e2", "e4", "e3", "e0", "e8", "e7", "e5")
    jedis.sadd("bb", "e1", "e2", "e4", "e3", "e0", "e8")
    //将aa中删除e1并存入bb中
    jedis.smove("aa","bb","e1")
    //aa和bb的交集
    jedis.sinter("aa","bb")
    //aa 和 bb 的并集
    jedis.sunion("aa","bb")
    //aa和bb的差集
    //aa中有，bb中没有
    jedis.sdiff("aa","bb")


    /* hash */
    //Redis hash 是一个键值(key=>value)对集合。
    //Redis hash是一个string类型的field和value的映射表，hash特别适合用于存储对象。
    val map = new util.HashMap[String, String]
    map.put("key001", "value001")
    map.put("key002", "value002")
    map.put("key003", "value003")
    jedis.hmset("hash", map)
    jedis.hset("hash", "key004", "value004")
    //散列hash的所有键值对为
    jedis.hgetAll("hash")
    //散列hash的所有键为
    jedis.hkeys("hash")
    //散列hash的所有值为
    jedis.hvals("hash")
    //将key006保存的值加上一个整数，如果key006不存在则添加key006
    jedis.hincrBy("hash","key006",6)
    //将key006保存的值加上一个整数，如果key006不存在则添加key006
    //结果 key006=9
    jedis.hincrBy("hash", "key006", 3)
    //删除一个或者多个键值对
    jedis.hdel("hash", "key002")
    //散列hash中键值对的个数
    jedis.hlen("hash")
    //判断hash中是否存在key002
    jedis.hexists("hash", "key002")
    //获取hash中的值
    jedis.hmget("hash", "key003","key004")


    /* 有序集合zset */
    //zset(sorted set：有序集合)
    //Redis zset 和 set 一样也是string类型元素的集合,且不允许重复的成员。
    //不同的是每个元素都会关联一个double类型的分数。redis正是通过分数来为集合中的成员进行从小到大的排序。
    val hashMap = new util.HashMap[String, Double]
    hashMap.put("key2", 1.2)
    hashMap.put("key3", 4.0)
    hashMap.put("key4", 5.0)
    hashMap.put("key5", 0.2)
    //添加元素
    jedis.zadd("zset", 3, "key1")
//    jedis.sadd("zset",hashMap)
    //zset中的所有元素
    jedis.zrange("zset", 0, -1)
    //zset中的所有元素
    jedis.zrange("zset", 0, -1)
    jedis.zrangeWithScores("zset", 0, -1)
    jedis.zrangeByScore("zset", 0, 100)
    jedis.zrangeByScoreWithScores("zset", 0, 100)
    //zset中key2的分值
    jedis.zscore("zset", "key2")
    //zset中key2的排名
    jedis.zrank("zset", "key2")
    //删除zset中的元素key3
    jedis.zrem("zset", "key3")
    //zset中元素的个数
    jedis.zcard("zset")
    //zset中分值在1-4之间的元素的个数
    jedis.zcount("zset", 1, 4)
    //key2的分值加上5
    jedis.zincrby("zset", 5, "key2")
  }
}
