package com.yw.musichw.eds.machine

import java.util.Properties

import com.yw.musichw.util.ConfigUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 由EDS层数据，聚合得到面向机器主题的营收数据
  * EDS层数据表：
  * TW_MAC_BASEINFO_D   机器基础信息日全量表
  * TW_MAC_LOC_D        机器位置信息日统计全量表
  * TW_CNSM_BRIEF_D     消费退款订单流水日增量表
  * TW_USR_BASEINFO_D   活跃用户基础信息日增量表
  * 由以上数据进行统计得到 EDS层 TW_MAC_STAT_D 机器日统计表,并将每天的结果保存到对应的mysql中。
  *
  * @author yangwei
  */
object GenerateTwMacStatD {
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

    // 获取当日的 TW_MAC_BASEINFO_D 机器基础信息日全量表数据
    sparkSession.table("TW_MAC_BASEINFO_D").where(s"data_dt = ${analyticDate}")
      .createTempView("TW_MAC_BASEINFO_D")

    // 获取当日的 TW_MAC_LOC_D 机器位置信息表数据
    sparkSession.table("TW_MAC_LOC_D").where(s"data_dt = ${analyticDate}")
      .createTempView("TW_MAC_LOC_D")

    // 获取当日的 TW_CNSM_BRIEF_D 消费退款订单流水日增量表数据
    sparkSession.table("TW_CNSM_BRIEF_D").where(s"data_dt = ${analyticDate}")
      .createTempView("TW_CNSM_BRIEF_D")

    // 获取当日的 TW_USR_BASEINFO_D 活跃用户基础信息日增量表数据
    sparkSession.table("TW_USR_BASEINFO_D").where(s"data_dt = ${analyticDate}")
      .createTempView("TW_USR_BASEINFO_D")

    // 根据当日的 TW_CNSM_BRIEF_D 进行机器营收统计，注意：这里获取 ABN_TYP = 0 的数据，就是正常订单数据
    sparkSession.sql(
      """
        |select
        |  MID,                                 -- 机器ID
        |  PKG_ID,                              -- 套餐ID
        |  PAY_TYPE,                            -- 支付类型
        |  count(distinct UID) as CNSM_USR_CNT, -- 总消费用户数
        |  sum(COIN_CNT * COIN_PRC) as TOT_REV, -- 总营收
        |  count(ORDR_ID) as REV_ORDR_CNT       -- 总营收订单数
        |from TW_CNSM_BRIEF_D
        |where ABN_TYP = 0
        |group by MID, PKG_ID, PAY_TYPE
      """.stripMargin).createTempView("TEMP_REV")

    // 根据当日的 TW_CNSM_BRIEF_D 进行机器退款统计，注意：这里获取 ABN_TYP = 2 的数据，就是退款的订单
    sparkSession.sql(
      """
        |select
        |  MID,                                 -- 机器ID
        |  PKG_ID,                              -- 套餐ID
        |  PAY_TYPE,                            -- 支付类型
        |  count(distinct UID) as REF_USR_CNT,  -- 总退款用户数
        |  sum(COIN_CNT * COIN_PRC) as TOT_REF, -- 总退款
        |  count(ORDR_ID) as REF_ORDR_CNT       -- 总退款订单数
        |from TW_CNSM_BRIEF_D
        |where ABN_TYP = 2
        |group by MID, PKG_ID, PAY_TYPE
      """.stripMargin).createTempView("TEMP_REF")

    // 根据当日的 TW_USR_BASEINFO_D 统计每台机器新注册用户数
    sparkSession.sql(
      s"""
        |select
        |  REG_MID as MID,              -- 机器ID
        |  count(UID) as NEW_USR_CNT    -- 新增用户个数
        |from TW_USR_BASEINFO_D
        |where REG_DT = ${analyticDate}
        |group by REG_MID
      """.stripMargin).createTempView("TEMP_USR_NEW")

    // 基于以上表的信息，统计得到 TW_MAC_STAT_D 机器日统计表 信息
    sparkSession.sql(
      """
        |select
        |  A.MID,                                     -- 机器ID
        |  A.MAC_NM,                                  -- 机器名称
        |  A.PRDCT_TYP,                               -- 产品类型
        |  A.STORE_NM,                                -- 门店名称
        |  A.BUS_MODE,                                -- 运营模式
        |  A.PAY_SW,                                  -- 是否开通移动支付
        |  A.SCENCE_CATGY,                            -- 主场景分类
        |  A.SUB_SCENCE_CATGY,                        -- 子场景分类
        |  A.SCENE,                                   -- 主场景
        |  A.SUB_SCENE,                               -- 子场景
        |  A.BRND,                                    -- 主场景品牌
        |  A.SUB_BRND,                                -- 子场景品牌
        |  nvl(B.PRVC, A.PRVC) as PRVC,               -- 省份
        |  nvl(B.CTY, A.CTY) as CTY,                  -- 城市
        |  nvl(B.DISTRICT, A.AREA) as AREA,           -- 区县
        |  nvl(A.PRTN_NM, A.ADDR) as AGE_ID,          -- 代理人ID
        |  case when (A.INV_RATE+A.AGE_RATE+A.COM_RATE+A.PAR_RATE) is null then 25
        |    else A.INV_RATE end as INV_RATE,         -- 投资人分成比例
        |  case when (A.INV_RATE+A.AGE_RATE+A.COM_RATE+A.PAR_RATE) is null then 25
        |    else A.AGE_RATE end as AGE_RATE,         -- 代理人、联盟人分成比例
        |  case when (A.INV_RATE+A.AGE_RATE+A.COM_RATE+A.PAR_RATE) is null then 25
        |    else A.COM_RATE end as COM_RATE,         -- 公司分成比例
        |  case when (A.INV_RATE+A.AGE_RATE+A.COM_RATE+A.PAR_RATE) is null then 25
        |    else A.PAR_RATE end as PAR_RATE,         -- 合作方分成比例
        |  C.PKG_ID,                                  -- 套餐ID
        |  C.PAY_TYPE,                                -- 支付类型
        |  nvl(C.CNSM_USR_CNT, 0) as CNSM_USR_CNT,    -- 总消费用户数
        |  nvl(D.REF_USR_CNT, 0) as REF_USR_CNT,      -- 总退款用户数
        |  nvl(E.NEW_USR_CNT, 0) as NEW_USR_CNT,      -- 总新增用户数
        |  nvl(C.REV_ORDR_CNT, 0) as REV_ORDR_CNT,    -- 总营收订单数
        |  nvl(D.REF_ORDR_CNT, 0) as REF_ORDR_CNT,    -- 总退款订单数
        |  nvl(C.TOT_REV, 0) as TOT_REV,              -- 总营收
        |  nvl(D.TOT_REF, 0) as TOT_REF               -- 总退款
        |from TW_MAC_BASEINFO_D A                     -- 机器基础信息
        |left join TW_MAC_LOC_D B on A.MID = B.MID    -- 机器当日位置信息
        |left join TEMP_REV C on A.MID = C.MID        -- 机器当日营收信息
        |left join TEMP_REF D on A.MID = D.MID
        |   and C.MID = D.MID
        |   and C.PKG_ID = D.PKG_ID
        |   and C.PAY_TYPE = D.PAY_TYPE               -- 机器当日退款信息
        |left join TEMP_USR_NEW E on A.MID = E.MID    -- 机器当日新增用户信息
      """.stripMargin).createTempView("TEMP_MAC_RESULT")

    // 将数据加载到对应的 EDS层 TW_MAC_STAT_D 分区表中
    sparkSession.sql(
      s"""
        |insert overwrite table TW_MAC_STAT_D partition (data_dt = ${analyticDate}) select * from TEMP_MAC_RESULT
      """.stripMargin)

    // 将以上结果保存至 MySQL 的song_result库中的 tm_machine_rev_infos 中，作为 DM 层结果
    val props = new Properties()
    props.setProperty("user", mysqlUser)
    props.setProperty("password", mysqlPassword)
    props.setProperty("driver", "com.mysql.jdbc.Driver")
    sparkSession.sql(s"select ${analyticDate} as data_dt, * from TEMP_MAC_RESULT")
        .write.mode(SaveMode.Overwrite).jdbc(mysqlUrl, "tm_machine_rev_infos", props)

    println("**** all finished ****")
  }
}
