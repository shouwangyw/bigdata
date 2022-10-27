package com.yw.musichw.streaming

import com.alibaba.fastjson.JSON
import com.yw.musichw.base.RedisClient
import com.yw.musichw.util.ConfigUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.mutable

/**
  * 实时获取用户点播歌曲日志，每隔10秒，获取最近10分钟歌曲的点播热度，并将结果存入MySQL中。
  *
  * @author yangwei
  */
object RealTimeHotSong {
  private val localRun: Boolean = ConfigUtils.LOCAL_RUN
  private val kafkaCluster = ConfigUtils.KAFKA_CLUSTER
  private val topicUserPlaySong = ConfigUtils.KAFKA_TOPIC_USER_PLAY_SONG

  private val mysqlUrl = ConfigUtils.MYSQL_URL
  private val mysqlUser = ConfigUtils.MYSQL_USER
  private val mysqlPassword = ConfigUtils.MYSQL_PASSWORD

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

    val ssc = new StreamingContext(sc, Durations.seconds(10))
    // 从redis中获取消费者offset
    val currentTopicOffset: mutable.Map[String, String] = RedisClient.getOffsetFromRedis(redisOffsetDb, topicUserPlaySong)
    currentTopicOffset.foreach(tp => println(s"初始读取到的topic offset: $tp"))
    // 转换成需要的类型
    val fromOffsets: Predef.Map[TopicPartition, Long] = currentTopicOffset.map {
      resultSet => new TopicPartition(topicUserPlaySong, resultSet._1.toInt) -> resultSet._2.toLong
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

    // 实时统计热门歌曲
    stream.map(cr => {
      val jsonObject = JSON.parseObject(cr.value())
      val songId = jsonObject.getString("songid")
      val songName = jsonObject.getString("songname")
      (songName, 1)
    }).reduceByKeyAndWindow((v1: Int, v2: Int) => {
      v1 + v2
    }, Durations.minutes(10), Durations.seconds(10))
        .foreachRDD(rdd => {
          println("current is start ... ...")
          // 将top30热度最高的歌曲，结果保存到MySQL表hot_song中
          val hotSongInfo: RDD[HotSongInfo] = rdd.map(tp => {
            HotSongInfo(tp._1, tp._2)
          })
          val session = sparkSession.newSession()
          import session.implicits._
          hotSongInfo.toDF().createTempView("temp_song_info")
          session.sql(
            """
              |select songname, times, row_number() over (partition by 1 order by times desc) as rank
              |from temp_song_info
            """.stripMargin)
              .filter("rank <= 30")
              .write.format("jdbc")
              .mode(SaveMode.Overwrite)
              .option("url", mysqlUrl)
              .option("user", mysqlUser)
              .option("password", mysqlPassword)
              .option("driver", "com.mysql.jdbc.Driver")
              .option("dbtable", "hot_song")
              .save()
          println("current is finished ... ...")
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
}
case class HotSongInfo(songName:String,times:Int)