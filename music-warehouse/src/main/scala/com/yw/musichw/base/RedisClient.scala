package com.yw.musichw.base

import com.yw.musichw.streaming.RealTimePVUV.topicUserLogin
import com.yw.musichw.util.ConfigUtils
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.JedisPool

/**
  *
  * @author yangwei
  */
object RedisClient {
  val redisHost = ConfigUtils.REDIS_HOST
  val redisPort = ConfigUtils.REDIS_PORT
  val redisTimeout = 30000

  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)

  def getOffsetFromRedis(db: Int, topic: String) = {
    val jedis = pool.getResource
    jedis.select(db)
    val result = jedis.hgetAll(topic)
    pool.returnResource(jedis)
    if (result.size() == 0) {
      result.put("0", "0")
      result.put("1", "0")
      result.put("2", "0")
    }
    import scala.collection.JavaConversions.mapAsScalaMap
    val offsetMap: scala.collection.mutable.Map[String, String] = result
    offsetMap
  }

  def saveOffsetToRedis(db: Int, offsetRanges: Array[OffsetRange]) = {
    val jedis = pool.getResource
    jedis.select(db)
    offsetRanges.foreach(offsetRange => {
      println(s"topic:${offsetRange.topic} partition:${offsetRange.partition} fromOffset:${offsetRange.fromOffset} untilOffset: ${offsetRange.untilOffset}")
      jedis.hset(offsetRange.topic, offsetRange.partition.toString, offsetRange.untilOffset.toString)
    })
    println("保存成功")
    pool.returnResource(jedis)
  }
}
