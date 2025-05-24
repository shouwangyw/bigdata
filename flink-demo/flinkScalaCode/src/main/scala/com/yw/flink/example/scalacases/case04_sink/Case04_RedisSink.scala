package com.yw.flink.example.scalacases.case04_sink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * Flink写出数据到Redis
  */
object Case04_RedisSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val result: DataStream[(String, Int)] = env.socketTextStream("node5", 9999)
      .flatMap(_.split(","))
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)

    //准备连接Redis配置
    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("node4")
      .setPort(6379)
      .setDatabase(2)
      .build()

    //准备RedisSink
    val redisSink = new RedisSink[(String, Int)](conf, new RedisMapper[(String, Int)] {
      //指定Redis的结构和key
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET, "flink-scala-redis")
      }

      //指定redis hset 中的key
      override def getKeyFromData(t: (String, Int)): String = t._1

      //指定redis hset中的value
      override def getValueFromData(t: (String, Int)): String = t._2.toString

    })

    result.addSink(redisSink)
    env.execute()
  }
}
