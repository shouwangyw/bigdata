package com.yw.musichw.eds.user

import java.util.Properties

import com.yw.musichw.util.{ConfigUtils, DateUtils}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  *
  * @author yangwei
  */
/**
  * 由ODS层以下几张表：
  * TO_YCAK_USR_D           微信用户全量表
  * TO_YCAK_USR_ALI_D       支付宝用户全量表
  * TO_YCAK_USR_QQ_D        QQ用户全量表
  * TO_YCAK_USR_APP_D       APP用户全量表
  * TO_YCAK_USR_LOGIN_D     用户登录数据表日增量表
  * 生成 EDS层 TW_USR_BASEINFO_D  用户基本信息日全量表
  * 同时向 MySQL 中生成7日活跃用户，DM层数据。
  */
object GenerateTwUsrBaseinfoD {
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
        .config("spark.sql.shuffle.partitions", "1")
        .config("hive.metastore.uris", hiveMetaStoreUris)
        .enableHiveSupport()
        .getOrCreate()
    } else {
      sparkSession = SparkSession.builder().config("spark.sql.shuffle.partitions", "1")
        .appName(this.getClass.getSimpleName).enableHiveSupport().getOrCreate()
    }

    val currentDate = args(0)

    sparkSession.sql(s"use $hiveDatabase")

    // 1. 获取微信全量用户信息，并注册对应的 TO_YCAK_USR_WX_D 视图
    val usrWx = sparkSession.sql(
      """
        | select
        |   UID,                            -- 用户ID
        |   REG_MID,                        -- 机器ID
        |   "1" AS REG_CHNL,                -- 1-微信渠道，2-支付宝渠道，3-QQ渠道，4-APP渠道
        |   WX_ID AS REF_UID,               -- 微信账号
        |   GDR,                            -- 性别
        |   BIRTHDAY,                       -- 生日
        |   MSISDN,                         -- 手机号码
        |   LOC_ID,                         -- 地区ID
        |   LOG_MDE,                        -- 注册登录方式
        |   substring(REG_TM,1,8) AS REG_DT,-- 注册日期
        |   substring(REG_TM,9,6) AS REG_TM,-- 注册时间
        |   USR_EXP,                        -- 用户当前经验值
        |   SCORE,                          -- 累计积分
        |   LEVEL,                          -- 用户等级
        |   "2" AS USR_TYPE,                -- 用户类型 1-企业 2-个人
        |   NULL AS IS_CERT,                -- 实名认证
        |   NULL AS IS_STDNT                -- 是否是学生
        | from TO_YCAK_USR_D
      """.stripMargin)

    // 2. 获取支付宝用户全量信息，并注册对应的 TO_YCAK_USR_ALI_D 视图
    val usrAli = sparkSession.sql(
      """
        | select
        |   UID,                            -- 用户ID
        |   REG_MID,                        -- 机器ID
        |   "2" AS REG_CHNL,                -- 1-微信渠道，2-支付宝渠道，3-QQ渠道，4-APP渠道
        |   ALY_ID AS REF_UID,              -- 支付宝账号
        |   GDR,                            -- 性别
        |   BIRTHDAY,                       -- 生日
        |   MSISDN,                         -- 手机号码
        |   LOC_ID,                         -- 地区ID
        |   LOG_MDE,                        -- 注册登录方式
        |   substring(REG_TM,1,8) AS REG_DT,-- 注册日期
        |   substring(REG_TM,9,6) AS REG_TM,-- 注册时间
        |   USR_EXP,                        -- 用户当前经验值
        |   SCORE,                          -- 累计积分
        |   LEVEL,                          -- 用户等级
        |   NVL(USR_TYPE,"2") AS USR_TYPE,  -- 用户类型 1-企业 2-个人
        |   IS_CERT,                        -- 实名认证
        |   IS_STDNT                        -- 是否是学生
        | from TO_YCAK_USR_ALI_D
      """.stripMargin)

    // 3. 获取QQ 用户全量信息 ，并注册对应的 TO_YCAK_USR_QQ_D 视图
    val usrQQ = sparkSession.sql(
      """
        | select
        |   UID,                            -- 用户ID
        |   REG_MID,                        -- 机器ID
        |   "3" AS REG_CHNL,                -- 1-微信渠道，2-支付宝渠道，3-QQ渠道，4-APP渠道
        |   QQID AS REF_UID,                -- QQ账号
        |   GDR,                            -- 性别
        |   BIRTHDAY,                       -- 生日
        |   MSISDN,                         -- 手机号码
        |   LOC_ID,                         -- 地区ID
        |   LOG_MDE,                        -- 注册登录方式
        |   substring(REG_TM,1,8) AS REG_DT,-- 注册日期
        |   substring(REG_TM,9,6) AS REG_TM,-- 注册时间
        |   USR_EXP,                        -- 用户当前经验值
        |   SCORE,                          -- 累计积分
        |   LEVEL,                          -- 用户等级
        |   "2" AS USR_TYPE,                -- 用户类型 1-企业 2-个人
        |   NULL AS IS_CERT,                -- 实名认证
        |   NULL AS IS_STDNT                -- 是否是学生
        | from TO_YCAK_USR_QQ_D
      """.stripMargin)

    // 4. 获取APP用户全量信息，并注册对应的 TO_YCAK_USR_APP_D 视图
    val usrApp = sparkSession.sql(
      """
        | select
        |   UID,                            -- 用户ID
        |   REG_MID,                        -- 机器ID
        |   "4" AS REG_CHNL,                -- 1-微信渠道，2-支付宝渠道，3-QQ渠道，4-APP渠道
        |   APP_ID AS REF_UID,              -- APP账号
        |   GDR,                            -- 性别
        |   BIRTHDAY,                       -- 生日
        |   MSISDN,                         -- 手机号码
        |   LOC_ID,                         -- 地区ID
        |   NULL LOG_MDE,                   -- 注册登录方式
        |   substring(REG_TM,1,8) AS REG_DT,-- 注册日期
        |   substring(REG_TM,9,6) AS REG_TM,-- 注册时间
        |   USR_EXP,                        -- 用户当前经验值
        |   0 as SCORE,                     -- 累计积分
        |   LEVEL,                          -- 用户等级
        |   "2" AS USR_TYPE,                -- 用户类型 1-企业 2-个人
        |   NULL AS IS_CERT,                -- 实名认证
        |   NULL AS IS_STDNT                -- 是否是学生
        | from TO_YCAK_USR_APP_D
      """.stripMargin)

    // 5. 获取平台所有用户信息
    val allUsrInfo = usrWx.union(usrAli).union(usrQQ).union(usrApp)

    /**
      * 6. 从 TO_YCAK_USR_LOGIN_D 用户登录数据增量表 获取当前登录的用户UID, 并对UID去重
      * 与所有用户信息关联获取当日用户详细信息
      */
    sparkSession.table("TO_YCAK_USR_LOGIN_D").where(s"data_dt = $currentDate")
      .select("UID")
      .distinct()
      .join(allUsrInfo, Seq("UID"), "left")
      .createTempView("TEMP_USR_ACTV")

    // 7. 将以上当日计算得到的活跃用户信息保存至 TW_USR_BASEINFO_D 日增量表中
    sparkSession.sql(
      s"""
        | insert overwrite table tw_usr_baseinfo_d partition (data_dt = ${currentDate})
        | select * from TEMP_USR_ACTV
      """.stripMargin)

    // 8. 获取7日 活跃用户信息 保存至 DM 层，保存到mysql songresult库下的 user_active
    val pre7Date = DateUtils.getCurrentDatePreDate(currentDate, 7)

    val props = new Properties()
    props.setProperty("user", mysqlUser)
    props.setProperty("password", mysqlPassword)
    props.setProperty("driver", "com.mysql.jdbc.Driver")

    sparkSession.sql(
      s"""
        | select
        |   A.UID,                                  -- 用户ID
        |   case when B.REG_CHNL = '1' THEN '微信'
        |        when B.REG_CHNL = '2' THEN '支付宝'
        |        when B.REG_CHNL = '3' THEN 'QQ'
        |        when B.REG_CHNL = '4' THEN 'APP'
        |        else '未知' end REG_CHNL,           -- 注册渠道
        |   B.REF_UID,                              -- 账号ID
        |   case when B.GDR = '0' THEN '不明'
        |        when B.GDR = '1' THEN '男'
        |        when B.GDR = '2' THEN '女'
        |        else '不明' end GDR,                -- 性别
        |   B.BIRTHDAY,                             -- 生日
        |   B.MSISDN,                               -- 手机号码
        |   B.REG_DT,                               -- 注册日期
        |   B.LEVEL                                 -- 用户等级
        | from (
        |   select UID, count(*) as c from TW_USR_BASEINFO_D
        |   where data_dt between ${pre7Date} and ${currentDate}
        |   group by UID having c = 1
        | ) A, TW_USR_BASEINFO_D B
        | where B.data_dt = ${currentDate} and A.UID = B.UID
      """.stripMargin).write.mode(SaveMode.Overwrite).jdbc(mysqlUrl, "user_7days_active", props)

    println("**** all finished ****")
  }
}
