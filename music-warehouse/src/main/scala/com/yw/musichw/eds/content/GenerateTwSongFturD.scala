package com.yw.musichw.eds.content

import com.yw.musichw.util.{ConfigUtils, DateUtils}
import org.apache.spark.sql.SparkSession

/**
  * 统计得到 TW 层的 TW_SONG_FTUR_D 表
  * 结合 TW_SONG_BASEINFO_D 与 TO_CLIENT_SONG_PLAY_OPERATE_REQ_D 生成 TW_SONG_FTUR_D
  *
  * @author yangwei
  */
object GenerateTwSongFturD {
  private val localRun: Boolean = ConfigUtils.LOCAL_RUN
  private val hiveMetastoreUris = ConfigUtils.HIVE_METASTORE_URIS
  private val hiveDatebase = ConfigUtils.HIVE_DATABASE
  private var sparkSession: SparkSession = _

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println(s"请输入数据日期,格式例如：年月日(20221230)")
      System.exit(1)
    }
    if (localRun) {
      sparkSession = SparkSession.builder().master("local")
          .appName(GenerateTwSongFturD.getClass.getSimpleName)
          .config("spark.sql.shuffle.partitions", "1")
          .config("hive.metastore.uris", hiveMetastoreUris)
          .enableHiveSupport()
          .getOrCreate()
      sparkSession.sparkContext.setLogLevel("Error")
    } else {
      sparkSession = SparkSession.builder().appName(GenerateTwSongFturD.getClass.getSimpleName)
          .enableHiveSupport().getOrCreate()
    }

    // 输入数据的日期，格式：年月日 xxxxxxxx
    val analyticDate = args(0)
    // 获取当前日期的前7天的日期
    val pre7Date = DateUtils.getCurrentDatePreDate(analyticDate, 7)
    // 获取当前日期的前30天的日期
    val pre30Date = DateUtils.getCurrentDatePreDate(analyticDate, 30)

    println(s"输入日期为: ${analyticDate}")
    println(s"pre7Date = ${pre7Date}")
    println(s"pre30Date = ${pre30Date}")

    sparkSession.sql(s"use $hiveDatebase")
    /*
      获取当天歌曲点唱统计: 基于 TO_CLIENT_SONG_PLAY_OPERATE_REQ_D 当前数据 统计当天歌曲点唱
     */
    sparkSession.sql(
      s"""
         | select
         |    songid as NBR,                        --歌曲ID
         |    count(*) as SING_CNT,                 --当日点唱量
         |    0 as SUPP_CNT,                        --当日点赞量
         |    count(distinct uid) as USR_CNT,       --当日点唱用户数
         |    count(distinct order_id) as ORDR_CNT  --当日点唱订单数
         | from to_client_song_play_operate_req_d
         | where data_dt = ${analyticDate}
         | group by songid
       """.stripMargin).createTempView("currentDayTable")

    /**
      * 获取近7天的歌曲点唱统计
      */
    sparkSession.sql(
      s"""
         | select
         |    songid as NBR,                              --歌曲ID
         |    count(*) as RCT_7_SING_CNT,                 --近七天点唱量
         |    0 as RCT_7_SUPP_CNT,                        --近七天点赞量
         |    count(distinct uid) as RCT_7_USR_CNT,       --近七天点唱用户数
         |    count(distinct order_id) as RCT_7_ORDR_CNT  --近七天点唱订单数
         | from to_client_song_play_operate_req_d
         | where data_dt >= ${pre7Date} and data_dt <= ${analyticDate}
         | group by songid
       """.stripMargin).createTempView("pre7DayTable")

    /**
      * 获取近30天的歌曲点唱统计
      */
    sparkSession.sql(
      s"""
         | select
         |    songid as NBR,                               --歌曲ID
         |    count(*) as RCT_30_SING_CNT,                 --近三十天点唱量
         |    0 as RCT_30_SUPP_CNT,                        --近三十天点赞量
         |    count(distinct uid) as RCT_30_USR_CNT,       --近三十天点唱用户数
         |    count(distinct order_id) as RCT_30_ORDR_CNT  --近三十天点唱订单数
         | from to_client_song_play_operate_req_d
         | where data_dt >= ${pre30Date} and data_dt <= ${analyticDate}
         | group by songid
       """.stripMargin).createTempView("pre30DayTable")

    /**
      * 从 TW_SONG_FTUR_D 表中获取过去7天和30天中 每首歌曲的 最高点唱量及点赞量
      */
    sparkSession.sql(
      s"""
         | select
         |    NBR, --歌曲ID
         |    max(case when DATA_DT BETWEEN ${pre7Date} and ${analyticDate} then SING_CNT else 0 end) as RCT_7_TOP_SING_CNT, --近七天最高日点唱量
         |    max(case when DATA_DT BETWEEN ${pre7Date} and ${analyticDate} then SUPP_CNT else 0 end) as RCT_7_TOP_SUPP_CNT, --近七天最高日点赞量
         |    max(SING_CNT) as RCT_30_TOP_SING_CNT,   --近三十天最高日点唱量
         |    max(SUPP_CNT) as RCT_30_TOP_SUPP_CNT    --近三十天最高日点赞量
         | from TW_SONG_FTUR_D
         | where DATA_DT BETWEEN ${pre30Date} and ${analyticDate}
         | group by NBR
       """.stripMargin).createTempView("pre7And30DayInfoTable")

    sparkSession.sql(
      s"""
         | select
         |    A.NBR,          --歌曲编号
         |    B.NAME,         --歌曲名称
         |    B.SOURCE,       --来源
         |    B.ALBUM,        --所属专辑
         |    B.PRDCT,        --发行公司
         |    B.LANG,         --语言
         |    B.VIDEO_FORMAT, --视频风格
         |    B.DUR,          --时长
         |    B.SINGER1,      --歌手1
         |    B.SINGER2,      --歌手2
         |    B.SINGER1ID,    --歌手1ID
         |    B.SINGER2ID,    --歌手2ID
         |    B.MAC_TIME,     --加入机器时间
         |    A.SING_CNT,     --当日点唱量
         |    A.SUPP_CNT,     --当日点赞量
         |    A.USR_CNT,      --当日点唱用户数
         |    A.ORDR_CNT,     --当日点唱订单数
         |    nvl(C.RCT_7_SING_CNT,0) as RCT_7_SING_CNT,            --近7天点唱量
         |    nvl(C.RCT_7_SUPP_CNT,0) as RCT_7_SUPP_CNT,            --近7天点赞量
         |    nvl(E.RCT_7_TOP_SING_CNT,0) as RCT_7_TOP_SING_CNT,    --近7天最高点唱量
         |    nvl(E.RCT_7_TOP_SUPP_CNT,0) as RCT_7_TOP_SUPP_CNT,    --近7天最高点赞量
         |    nvl(C.RCT_7_USR_CNT,0) as RCT_7_USR_CNT,              --近7天点唱用户数
         |    nvl(C.RCT_7_ORDR_CNT,0) as RCT_7_ORDR_CNT,            --近7天点唱订单数
         |    nvl(D.RCT_30_SING_CNT,0) as RCT_30_SING_CNT,          --近30天点唱量
         |    nvl(D.RCT_30_SUPP_CNT,0) as RCT_30_SUPP_CNT,          --近30天点赞量
         |    nvl(E.RCT_30_TOP_SING_CNT,0) as RCT_30_TOP_SING_CNT,  --近30天最高点唱量
         |    nvl(E.RCT_30_TOP_SUPP_CNT,0) as RCT_30_TOP_SUPP_CNT,  --近30天最高点赞量
         |    nvl(D.RCT_30_USR_CNT,0) as RCT_30_USR_CNT,            --近30天点唱用户数
         |    nvl(D.RCT_30_ORDR_CNT,0) as RCT_30_ORDR_CNT           --近30天点唱订单数
         | from currentDayTable A
         | JOIN TW_SONG_BASEINFO_D B ON A.NBR = B.NBR
         | LEFT JOIN pre7DayTable C ON A.NBR = C.NBR
         | LEFT JOIN pre30DayTable D ON A.NBR = D.NBR
         | LEFT JOIN pre7And30DayInfoTable E ON A.NBR = E.NBR
      """.stripMargin).createTempView("result")

    sparkSession.sql(
      s"""
         | insert overwrite table tw_song_ftur_d partition(data_dt=${analyticDate}) select * from result
       """.stripMargin)

    println("**** all finished ****")
  }
}
