package com.yw.musichw.dm.content

import java.util.Properties

import com.yw.musichw.util.ConfigUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 根据 EDS层 TW_MAC_STAT_D 机器营收日统计表 统计得到 TM_USR_MRCHNT_STAT_D 商户营收日统计表
  *
  * @author yangwei
  */
object GenerateTmUsrMrchntStatD {
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

    // 查询 EDS层 TW_MAC_STAT_D 机器营收日统计表，统计用户营收情况
    sparkSession.sql(
      s"""
         |select
         |  AGE_ID as ADMIN_ID,                                                     -- 代理人
         |  PAY_TYPE,
         |  sum(REV_ORDR_CNT) as REV_ORDR_CNT,                                      -- 总营收订单数
         |  sum(REF_ORDR_CNT) as REF_ORDR_CNT,                                      -- 总退款订单数
         |  cast(sum(TOT_REV) as Double) as TOT_REV,                                -- 总营收
         |  cast(sum(TOT_REF) as Double) as TOT_REF,                                -- 总退款
         |  cast(sum(TOT_REV * nvl(INV_RATE, 0) / 100) as DECIMAL(10, 4)) as TOT_INV_REV, -- 投资人营收
         |  cast(sum(TOT_REV * nvl(AGE_RATE, 0) / 100) as DECIMAL(10, 4)) as TOT_AGE_REV, -- 代理人营收
         |  cast(sum(TOT_REV * nvl(COM_RATE, 0) / 100) as DECIMAL(10, 4)) as TOT_COM_REV, -- 公司营收
         |  cast(sum(TOT_REV * nvl(PAR_RATE, 0) / 100) as DECIMAL(10, 4)) as TOT_PAR_REV  -- 合伙人营收
         |from TW_MAC_STAT_D
         |where data_dt = ${analyticDate}
         |group by age_id, pay_type
       """.stripMargin).createTempView("TEMP_USR_MRCHNT_STAT")

    // 将以上结果保存到分区表 TM_USR_MRCHNT_STAT_D 商户日营收统计表 中
    sparkSession.sql(
      s"""
         |insert overwrite table TM_USR_MRCHNT_STAT_D partition (data_dt = ${analyticDate}) select * from TEMP_USR_MRCHNT_STAT
       """.stripMargin)

    // 同时将以上结果保存至 MySQL song_result 库中的 tm_usr_mrchnt_stat_d 中，作为DM层结果
    val props = new Properties()
    props.setProperty("user", mysqlUser)
    props.setProperty("password", mysqlPassword)
    props.setProperty("driver", "com.mysql.jdbc.Driver")
    sparkSession.sql(
      s"""
         |select ${analyticDate} as data_dt, * from TEMP_USR_MRCHNT_STAT
       """.stripMargin).write.mode(SaveMode.Overwrite).jdbc(mysqlUrl, "tm_usr_mrchnt_stat_d", props)

    println("**** all finished ****")
  }
}
