package com.yw.musichw.dm.content

import java.util.Properties

import com.yw.musichw.util.ConfigUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 根据 TW_SONG_FTUR_D 表得到 TW_SONG_RSI_D 歌曲影响力指数日统计表，同时得到 MySQL 中的 tm_song_rsi 表
  *
  * @author yangwei
  */
object GenerateTmSongRsiD {
  private val localRun: Boolean = ConfigUtils.LOCAL_RUN
  private val hiveMetastoreUris = ConfigUtils.HIVE_METASTORE_URIS
  private val hiveDatabase = ConfigUtils.HIVE_DATABASE
  private var sparkSession: SparkSession = _

  private val mysqlUrl = ConfigUtils.MYSQL_URL
  private val mysqlUser = ConfigUtils.MYSQL_USER
  private val mysqlPassword = ConfigUtils.MYSQL_PASSWORD

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println(s"请输入数据日期,格式例如：年月日(20221230)")
      System.exit(1)
    }
    if (localRun) {
      sparkSession = SparkSession.builder().master("local")
        .appName(this.getClass.getSimpleName)
        .config("spark.sql.shuffle.partitions", "1")
        .config("hive.metastore.uris", hiveMetastoreUris)
        .enableHiveSupport()
        .getOrCreate()
      sparkSession.sparkContext.setLogLevel("Error")
    } else {
      sparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
        .enableHiveSupport().getOrCreate()
    }

    val currentDate = args(0)

    sparkSession.sql(s"use $hiveDatabase")
    val dataFrame = sparkSession.sql(
      s"""
         | select
         |    data_dt,                    --日期
         |    NBR,                        --歌曲ID
         |    NAME,                       --歌曲名称
         |    SING_CNT,                   --当日点唱量
         |    SUPP_CNT,                   --当日点赞量
         |    RCT_7_SING_CNT,             --近七日点唱量
         |    RCT_7_SUPP_CNT,             --近七日点赞量
         |    RCT_7_TOP_SING_CNT,         --近七日最高点唱量
         |    RCT_7_TOP_SUPP_CNT,         --近七日最高点赞量
         |    RCT_30_SING_CNT,            --近三十日点唱量
         |    RCT_30_SUPP_CNT,            --近三十日点赞量
         |    RCT_30_TOP_SING_CNT,        --近三十日最高点唱量
         |    RCT_30_TOP_SUPP_CNT         --近三十日最高点赞量
         | from TW_SONG_FTUR_D
         | where data_dt = ${currentDate}
       """.stripMargin)

    import org.apache.spark.sql.functions._

    /**
      * 日周期-整体影响力
      * 7日周期-整体影响力
      * 30日周期-整体影响力
      */
    dataFrame.withColumn("RSI_1D", pow(log(col("SING_CNT") / 1 + 1) * 0.63 * 0.8 + log(col("SUPP_CNT") / 1 + 1) * 0.63 * 0.2, 2) * 10)
      .withColumn("RSI_7D", pow((log(col("RCT_7_SING_CNT") / 7 + 1) * 0.63 + log(col("RCT_7_TOP_SING_CNT") + 1) * 0.37) * 0.8
        + (log(col("RCT_7_SUPP_CNT") / 7 + 1) * 0.63 + log(col("RCT_7_TOP_SUPP_CNT") + 1) * 0.37) * 0.2, 2) * 10)
      .withColumn("RSI_30D", pow((log(col("RCT_30_SING_CNT") / 30 + 1) * 0.63 + log(col("RCT_30_TOP_SING_CNT") + 1) * 0.37) * 0.8
        + (log(col("RCT_30_SUPP_CNT") / 30 + 1) * 0.63 + log(col("RCT_30_TOP_SUPP_CNT") + 1) * 0.37) * 0.2, 2) * 10)
      .createTempView("TEMP_TW_SONG_FTUR_D")

    val rsi_1d = sparkSession.sql(
      s"""
         | select
         |    "1" as PERIOD, NBR, NAME, RSI_1D AS RSI,
         |    row_number() over(partition by data_dt order by RSI_1D desc) as RSI_RANK
         | from TEMP_TW_SONG_FTUR_D
       """.stripMargin)
    val rsi_7d = sparkSession.sql(
      s"""
         | select
         |    "7" as PERIOD, NBR, NAME, RSI_7D AS RSI,
         |    row_number() over(partition by data_dt order by RSI_7D desc) as RSI_RANK
         | from TEMP_TW_SONG_FTUR_D
       """.stripMargin)
    val rsi_30d = sparkSession.sql(
      s"""
         | select
         |    "30" as PERIOD, NBR, NAME, RSI_30D AS RSI,
         |    row_number() over(partition by data_dt order by RSI_30D desc) as RSI_RANK
         | from TEMP_TW_SONG_FTUR_D
       """.stripMargin)

    rsi_1d.union(rsi_7d).union(rsi_30d).createTempView("result")

    sparkSession.sql(
      s"""
         | insert overwrite table TW_SONG_RSI_D partition(data_dt = ${currentDate}) select * from result
       """.stripMargin)

    // 这里把排名前30的数据保存到MySQL数据库
    val props = new Properties()
    props.setProperty("user", mysqlUser)
    props.setProperty("password", mysqlPassword)
    props.setProperty("driver", "com.mysql.jdbc.Driver")
    sparkSession.sql(
      s"""
         | select ${currentDate} as data_dt, PERIOD, NBR, NAME, RSI, RSI_RANK from result where rsi_rank <= 30
       """.stripMargin).write.mode(SaveMode.Overwrite).jdbc(mysqlUrl, "tm_song_rsi", props)

    println("**** all finished ****")
  }
}
