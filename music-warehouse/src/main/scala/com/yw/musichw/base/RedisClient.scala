package com.yw.musichw.base

import com.yw.musichw.util.ConfigUtils
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
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
}
