package com.yw.musichw.streaming

import com.yw.musichw.util.ConfigUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * 实时获取用户点播歌曲日志，每隔30秒，获取最近10分钟歌曲的点播热度，并将结果存入MySQL中。
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

    val ssc = new StreamingContext(sc, Durations.seconds(20))
  }
}
