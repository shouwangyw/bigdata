package com.yw.flink.example._01_datasource_sink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * Flink实时程序处理保存结果到redis中
  */
object Stream2Redis {
  def main(args: Array[String]): Unit = {
    // 1. 构建流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 2. 数组数据
    val streamSource: DataStream[String] = env.fromElements("1 hadoop", "2 spark", "3 flink")

    // 3. 数据处理
    val tupleValue: DataStream[(String, String)] = streamSource.map(x => (x.split(" ")(0), x.split(" ")(1)))

    // 4. 构建RedisSink
    val builder = new FlinkJedisPoolConfig.Builder
    // 设置 redis 客户端参数
    builder.setHost("node01")
    builder.setPort(6379)
    builder.setTimeout(5000)
    builder.setMaxTotal(50)
    builder.setMaxIdle(10)
    builder.setMinIdle(5)
    val config: FlinkJedisPoolConfig = builder.build()
    // 获取redis sink
    val redisSink = new RedisSink[Tuple2[String, String]](config, new MyRedisMapper)

    // 5. 使用自定义Sink，实现数据写入到 redis 中
    tupleValue.addSink(redisSink)

    // 6. 执行程序
    env.execute("redisSink")
  }
}
/**
  * 定义一个RedisMapper类
  */
class MyRedisMapper extends RedisMapper[Tuple2[String, String]] {
  override def getCommandDescription: RedisCommandDescription = {
    // 设置插入数据到 redis 的命令
    new RedisCommandDescription(RedisCommand.SET)
  }

  override def getKeyFromData(data: (String, String)): String = {
    // 指定 key
    data._1
  }

  override def getValueFromData(data: (String, String)): String = {
    // 指定 value
    data._2
  }
}
