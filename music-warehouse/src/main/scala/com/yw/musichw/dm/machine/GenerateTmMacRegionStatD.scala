package com.yw.musichw.dm.machine

import java.util.Properties

import com.yw.musichw.util.ConfigUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 根据 EDS 层 TW_MAC_STAT_D 机器营收日统计表，得到 TM_MAC_REGION_STAT_D 地区营收日统计表
  *
  * @author yangwei
  */
object GenerateTmMacRegionStatD {
  val localRun: Boolean = ConfigUtils.LOCAL_RUN
  val hiveMetaStoreUris = ConfigUtils.HIVE_METASTORE_URIS
  val hiveDatabase = ConfigUtils.HIVE_DATABASE
  var sparkSession: SparkSession = _

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
        .config("spark.sql.shuffle.partitions", "10")
        .config("hive.metastore.uris", hiveMetaStoreUris)
        .enableHiveSupport()
        .getOrCreate()
    } else {
      sparkSession = SparkSession.builder().config("spark.sql.shuffle.partitions", "10")
        .appName(this.getClass.getSimpleName).enableHiveSupport().getOrCreate()
    }

    val analyticDate = args(0)
    sparkSession.sparkContext.setLogLevel("Error")
    sparkSession.sql(s"use $hiveDatabase")

    sparkSession.sql(
      s"""
         |select
         |  PRVC,                                               -- 省份
         |  CTY,                                                -- 城市
         |  count(MID) as MAC_CNT,                              -- 机器数量
         |  cast(sum(TOT_REV) as DECIMAL(10, 4)) as MAC_REV,    -- 总营收
         |  cast(sum(TOT_REF) as DECIMAL(10, 4)) as MAC_REF,    -- 总退款
         |  sum(REV_ORDR_CNT) as MAC_REV_ORDR_CNT,              -- 总营收订单数
         |  sum(REF_ORDR_CNT) as MAC_REF_ORDR_CNT,              -- 总退款订单数
         |  sum(CNSM_USR_CNT) as MAC_CNSM_USR_CNT,              -- 总消费用户数
         |  sum(REF_USR_CNT) as MAC_REF_USR_CNT                 -- 总退款用户数
         |from TW_MAC_STAT_D
         |where data_dt = ${analyticDate}
         |group by PRVC, CTY
       """.stripMargin).createTempView("TEMP_MAC_REGION_STAT")

    // 将以上结果保存到分区表 TM_MAC_REGION_STAT_D 地区营收日统计表
    sparkSession.sql(
      s"""
         |insert overwrite table TM_MAC_REGION_STAT_D partition (data_dt = ${analyticDate}) select * from TEMP_MAC_REGION_STAT
       """.stripMargin)

    // 将以上结果保存至MySQL song_result 库中 tm_mac_region_stat_d 中，作为 DM层 结果
    val props = new Properties()
    props.setProperty("user", mysqlUser)
    props.setProperty("password", mysqlPassword)
    props.setProperty("driver", "com.mysql.jdbc.Driver")
    sparkSession.sql(
      s"""
         |select ${analyticDate} as data_dt, * from TEMP_MAC_REGION_STAT
       """.stripMargin).write.mode(SaveMode.Overwrite).jdbc(mysqlUrl, "tm_mac_region_stat_d", props)

    println("**** all finished ****")
  }
}
