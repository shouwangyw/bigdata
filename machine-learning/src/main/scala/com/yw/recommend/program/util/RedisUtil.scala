package com.yw.recommend.program.util

import redis.clients.jedis.{Jedis, JedisPool}
import redis.clients.util.Pool

object RedisUtil {
  private[this] var jedisPool: Pool[Jedis] = _

  def init(host: String, port: Int): Unit = {
    jedisPool = new JedisPool(host, port)
  }

  def updateHot(dbIndex: Int, key: String, itemID: String): Boolean = {
    try {
      val jedis = jedisPool.getResource
      jedis.select(dbIndex)
      jedis.zincrby(key, 1, itemID)
      jedis.close()
      true
    } catch {
      case e: Exception => {
        false
      }
    }
  }
}
