package com.yw.musichw.streaming

import com.alibaba.fastjson.JSON
import com.yw.musichw.base.RedisClient
import com.yw.musichw.util.ConfigUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Durations, StreamingContext}
import redis.clients.jedis.Pipeline

import scala.collection.mutable

/**
  * 实时获取用户的登录系统数据，每隔5秒统计在线用户的PV,UV
  *
  * @author yangwei
  */
object RealTimePVUV {
  private val localRun: Boolean = ConfigUtils.LOCAL_RUN
  private val kafkaCluster = ConfigUtils.KAFKA_CLUSTER
  private val topicUserLogin = ConfigUtils.KAFKA_TOPIC_USER_LOGIN

  private val redisDb = ConfigUtils.REDIS_DB
  private val redisOffsetDb = ConfigUtils.REDIS_OFFSET_DB

  private var sparkSession: SparkSession = _
  private var sc: SparkContext = _

  def main(args: Array[String]): Unit = {
    if (localRun) {
      sparkSession = SparkSession.builder()
        .master("local")
        .appName(this.getClass.getSimpleName)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .enableHiveSupport().getOrCreate()
    } else {
      sparkSession = SparkSession.builder()
        .appName(this.getClass.getSimpleName)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    }
    sc = sparkSession.sparkContext
    sc.setLogLevel("Error")

    val ssc = new StreamingContext(sc, Durations.seconds(5))
    // 从redis中获取消费者offset
    val currentTopicOffset: mutable.Map[String, String] = RedisClient.getOffsetFromRedis(redisOffsetDb, topicUserLogin)
    currentTopicOffset.foreach(tp => println(s"初始读取到的topic offset: $tp"))
    // 转换成需要的类型
    val fromOffsets: Predef.Map[TopicPartition, Long] = currentTopicOffset.map {
      resultSet => new TopicPartition(topicUserLogin, resultSet._1.toInt) -> resultSet._2.toLong
    }.toMap

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaCluster,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "MyGroupId_1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean) // 默认是true
    )

    // 将获取到的消费者offset 传递给SparkStreaming
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
    )

    // 统计PV
    stream.map(cr => {
      val jsonObject = JSON.parseObject(cr.value())
      val mid = jsonObject.getString("mid")
      val uid = jsonObject.getString("uid")
      (mid, 1)
    }).reduceByKeyAndWindow((v1: Int, v2: Int) => {
      v1 + v2
    }, Durations.minutes(1), Durations.seconds(5)) // 每隔5s统计过去最近1分钟
      .foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        // 将结果存储在redis中，格式为K:pv, V:mid, pv
        savePVToRedis(redisDb, it)
      })
    })

    // 统计UV
    stream.window(Durations.minutes(1), Durations.seconds(5)).map(cr => {
      val jsonObject = JSON.parseObject(cr.value())
      val mid = jsonObject.getString("mid")
      val uid = jsonObject.getString("uid")
      (mid, uid)
    }).transform(rdd => {
      val distinctRDD = rdd.distinct()
      distinctRDD.map(tp => {
        (tp._1, 1)
      })
    }).reduceByKey((v1: Int, v2: Int) => {
      v1 + v2
    }).foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        // 将结果存储在redis中，格式为K:pv, V:mid, pv
        saveUVToRedis(redisDb, it)
      })
    })

    stream.foreachRDD { (rdd: RDD[ConsumerRecord[String, String]]) =>
      println("所有业务完成")
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      // 将当前批次最后的所有分区offsets 保存到 Redis中
      RedisClient.saveOffsetToRedis(redisOffsetDb, offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }


  def savePVToRedis(db: Int, it: Iterator[(String, Int)]) = {
    val jedis = RedisClient.pool.getResource
    jedis.select(db)
    // pipelined()方法获取管道,它可以实现一次性发送多条命令并一次性返回结果,这样就大量的减少了客户端与Redis的通信次数
    val pipeline: Pipeline = jedis.pipelined()
    it.foreach(tp => {
      pipeline.hset("pv", tp._1, tp._2.toString)
    })
    pipeline.sync()
    println("PV保存成功")
    RedisClient.pool.returnResource(jedis)
  }

  def saveUVToRedis(db: Int, it: Iterator[(String, Int)]) = {
    val jedis = RedisClient.pool.getResource
    jedis.select(db)
    val pipeline: Pipeline = jedis.pipelined()
    it.foreach(tp => {
      pipeline.hset("uv", tp._1, tp._2.toString)
    })
    pipeline.sync()
    println("UV保存成功")
    RedisClient.pool.returnResource(jedis)
  }
}
