package com.yw.spark.example.structedstreaming.cases

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *  读取Kafka 实时数据与读取mysql 中静态数据进行关联，得到点播歌曲基本信息
  */
object Case22_ReduceSongInfo {
  def main(args: Array[String]): Unit = {
    // 1. 创建对象
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    // 2. 导入隐士转换，设置日志
    import spark.implicits._
    spark.sparkContext.setLogLevel("Error")

    //3.读取Mysql中歌曲基本信息数据
    val songBaseInfoDF: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://node2:3306/testdb")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "song")
      .load()

    //对数据进行缓存
//    songBaseInfoDF.persist()
    songBaseInfoDF.createTempView("song_base_info")
    spark.sqlContext.cacheTable("song_base_info")

    //4.读取Kafka 实时数据
    val songLogDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092,node2:9092,node3:9092")
      .option("startingOffsets", "latest")
      .option("subscribe", "playsong-topic")
      .load()

    import org.apache.spark.sql.functions._

    //5.对用户点播歌曲实时数据进行转换
    val songPlayDF: DataFrame = songLogDF.selectExpr("cast (key as string)", "cast (value as string)")
      .as[(String, String)]
      //{"songid": "10035283", "mid": 52949, "optrate_type": 0,
      // "uid": 49915734, "consume_type": 0, "play_time": 0,
      // "dur_time": 0, "session_id": 89210, "songname": "岁月神偷",
      // "pkg_id": 2, "order_id": "W20191203041257_3_49915734_52949"}
      .withColumn("songid", get_json_object(col("value"), "$.songid"))
      .withColumn("mid", get_json_object(col("value"), "$.mid"))
      .withColumn("uid", get_json_object(col("value"), "$.uid"))
      .withColumn("play_time", get_json_object(col("value"), "$.play_time"))
      .withColumn("dur_time", get_json_object(col("value"), "$.dur_time"))
      .withColumn("songname", get_json_object(col("value"), "$.songname"))
      .withColumn("order_id", get_json_object(col("value"), "$.order_id"))
      .drop("key", "value")
    songPlayDF.createTempView("song_play_info")

    val resultDF = spark.sql(
      """
        | select a.songid,a.mid,a.uid,a.play_time,a.dur_time,a.songname,a.order_id,get_json_object(b.singer_info,'$[0].name') as singer
        | from song_play_info a join song_base_info b
        | on a.songid = b.source_id
        | where a.order_id is not null and a.order_id != ""
      """.stripMargin)

    //5.join 关联数据，获取歌曲歌手信息
//    val resultDF: Dataset[Row] = songPlayDF.join(songBaseInfoDF, songPlayDF.col("songid") === songBaseInfoDF.col("source_id"))
//      .withColumn("singer", get_json_object(col("singer_info"), "$[0].name"))
//      .select("order_id", "songid", "mid", "songname", "singer")
//      .filter(col("order_id").isNotNull)
//      .where("order_id != ''")

    //6.将结果写出到Kafka topic
    val query: StreamingQuery = resultDF.map(row => {
      val orderId: String = row.getString(0)
      val songId: String = row.getString(1)
      val mid: String = row.getString(2)
      val songName: String = row.getString(3)
      val singer: String = row.getString(4)
      s"$orderId,$songId,$mid,$songName,$singer"
    }).writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092,node2:9092,node3:9092")
      .option("topic", "song-singer-result")
      .option("checkpointLocation", s".checkpoint/${System.currentTimeMillis()}")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    query.awaitTermination()
  }
}
