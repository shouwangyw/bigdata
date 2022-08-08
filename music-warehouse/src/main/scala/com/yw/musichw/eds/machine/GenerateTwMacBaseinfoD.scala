package com.yw.musichw.eds.machine

import java.util.Properties

import com.yw.musichw.util.{ConfigUtils, StringUtils}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 根据ODS层数据 生成 机器基础信息日全量表
  *
  * @author yangwei
  */
object GenerateTwMacBaseinfoD {
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

    val currentDate = args(0)

    sparkSession.sql(s"use $hiveDatabase")

    import org.apache.spark.sql.functions._ //导入函数，可以使用 udf、col 方法

    val udfGetFormatTime = udf(getFormatTime)
    val udfGetPrvcInfo = udf(getPrvcInfo)
    val udfGetCityInfo = udf(getCityInfo)

    // 1. 加载 TO_YCAK_MAC_D 机器基本信息表: 获取机器信息，并设置产品类型为2
    val df1: DataFrame = sparkSession.table("TO_YCAK_MAC_D")
      .withColumn("PRDCT_TYPE", lit(2))
    df1.createTempView("TO_YCAK_MAC_D")

    // 2. 加载 TO_YCBK_MAC_ADMIN_MAP_D 机器客户关系资料表: 获取机器信息，并将“ACTV_TM”、“ORDER_TM”进行格式化清洗和Null值的填补
    val df2: DataFrame = sparkSession.table("TO_YCBK_MAC_ADMIN_MAP_D")
      .withColumn("ACTV_TM", udfGetFormatTime(col("ACTV_TM")))
      .withColumn("ORDER_TM", udfGetFormatTime(col("ORDER_TM")))
    df2.createTempView("TO_YCBK_MAC_ADMIN_MAP_D")

    // 3. 由以上两张表 获取所有的 机器ID 信息,并注册临时视图 TEMP_MAC_ALL
    df1.select("MID", "PRDCT_TYPE").union(df2.select("MID", "PRDCT_TYPE"))
      .distinct().createTempView("TEMP_MAC_ALL")

    // 4. 读取 ycak 库下的 抽取到的 TO_YCAK_MAC_LOC_D 机器位置信息表 进行清洗
    //// 4.1 对表中的 PRVC、CTY 字段进行格式整理，对于省份和市不为空则使用原来的值，如果省份或城市为空值，由 ADDR 字段截取获得
    //// 4.2 过滤清洗数据之后，PRVC和CTY字段依然为null的数据，我们认为这类数据就是脏数据
    //// 4.3 对 REV_TM、SALE_TM字段进行清洗，将空值填补
    val prvcOrCtyIsNotNullDF = sparkSession.table("TO_YCAK_MAC_LOC_D")
      .filter("prvc != 'null' and cty != 'null'")
    sparkSession.table("TO_YCAK_MAC_LOC_D").filter("prvc = 'null' and cty = 'null'")
      .withColumn("PRVC", udfGetPrvcInfo(col("PRVC"), col("ADDR")))
      .withColumn("CTY", udfGetCityInfo(col("CTY"), col("ADDR")))
      .union(prvcOrCtyIsNotNullDF)
      .filter("prvc != '' and cty != ''")
      .withColumn("REV_TM", udfGetFormatTime(col("REV_TM")))
      .withColumn("SALE_TM", udfGetFormatTime(col("SALE_TM")))
      .createTempView("TO_YCAK_MAC_LOC_D")

    // 5. 获取 机器上歌曲版本 机器位置信息
    // 以 TEMP_MAC_ALL 中的MID 为基准，对 TO_YCAK_MAC_D、TO_YCAK_MAC_LOC_D两张表的信息进行统计
    sparkSession.sql(
      """
        | select
        |   TEMP.MID,           -- 机器ID
        |   MAC.SRL_ID,         -- 序列号
        |   MAC.HARD_ID,        -- 硬件ID
        |   MAC.SONG_WHSE_VER,  -- 歌库版本号
        |   MAC.EXEC_VER,       -- 系统版本号
        |   MAC.UI_VER,         -- 歌库UI版本号
        |   MAC.STS,            -- 激活状态
        |   MAC.CUR_LOGIN_TM,   -- 最近登录时间
        |   MAC.PAY_SW,         -- 支付开关是否打开
        |   MAC.IS_ONLINE,      -- 是否在线
        |   MAC.PRDCT_TYPE,     -- 产品类型，2
        |   LOC.PRVC,           -- 机器所在省份
        |   LOC.CTY,            -- 机器所在省份
        |   LOC.ADDR_FMT,       -- 详细地址
        |   LOC.REV_TM,         -- 运营时间
        |   LOC.SALE_TM         -- 销售时间
        | from TEMP_MAC_ALL as TEMP
        | left join TO_YCAK_MAC_D as MAC on TEMP.MID = MAC.MID
        | left join TO_YCAK_MAC_LOC_D as LOC on TEMP.MID = LOC.MID
      """.stripMargin).createTempView("TEMP_YCAK_MAC_INFO")

    // 6. 获取机器 套餐名称、投资分成、机器门店、场景信息
    // 以TEMP_MAC_ALL 表为基准， 对 ycbk 系统对应的ODS层数据 进行统计
    sparkSession.sql(
      """
        | select
        |   TEMP.MID,                   -- 机器ID
        |   MA.MAC_NM,                  -- 机器名称
        |   MA.PKG_NM,                  -- 套餐名称
        |   MA.INV_RATE,                -- 投资人分成比例
        |   MA.AGE_RATE,                -- 承接方分成比例
        |   MA.COM_RATE,                -- 公司分成比例
        |   MA.PAR_RATE,                -- 合作方分成比例
        |   MA.IS_ACTV,                 -- 是否激活
        |   MA.ACTV_TM,                 -- 激活时间
        |   MA.HAD_MPAY_FUNC as PAY_SW, -- 支付开关是否打开
        |   PRVC.PRVC,                  -- 省份
        |   CTY.CTY,                    -- 城市
        |   AREA.AREA,                  -- 区、县
        |   CONCAT(MA.SCENE_ADDR, MA.GROUND_NM) as ADDR,  -- 场景地址，场地名称
        |   STORE.GROUND_NM as STORE_NM,  -- 门店名称, 这里的store_nm都是数字
        |   STORE.TAG_NM,	              -- 主场景名称
        |   STORE.SUB_TAG_NM,	          -- 主场景分类
        |   STORE.SUB_SCENE_CATGY_NM,	  -- 子场景分类名称
        |   STORE.SUB_SCENE_NM,	        -- 子场景名称
        |   STORE.BRND_NM,	            -- 品牌名称
        |   STORE.SUB_BRND_NM 	        -- 子品牌名称
        | from TEMP_MAC_ALL as TEMP
        | left join TO_YCBK_MAC_ADMIN_MAP_D as MA on TEMP.MID = MA.MID
        | left join TO_YCBK_PRVC_D as PRVC on MA.SCENE_PRVC_ID = PRVC.PRVC_ID
        | left join TO_YCBK_CITY_D as CTY on MA.SCENE_CTY_ID = CTY.CTY_ID
        | left join TO_YCBK_AREA_D as AREA on MA.SCENE_AREA_ID = AREA.AREA_ID
        | left join TO_YCBK_MAC_STORE_MAP_D as SMA on TEMP.MID = SMA.MID
        | left join TO_YCBK_STORE_D as STORE on SMA.STORE_ID =  STORE.ID
      """.stripMargin).createTempView("TEMP_YCBK_MAC_INFO")

    // 7. 根据 TEMP_YCAK_MAC_INFO 和 TEMP_YCBK_MAC_INFO 关联得到结果表
    sparkSession.sql(
      """
        | select
        |   YCAK.MID,                                               -- 机器ID
        |   YCBK.MAC_NM,                                            -- 机器名称
        |   YCAK.SONG_WHSE_VER,                                     -- 歌曲版本
        |   YCAK.EXEC_VER,	                                        -- 系统版本号
        |   YCAK.UI_VER,		                                        -- 歌曲UI版本号
        |   YCAK.HARD_ID,                                           -- 硬件ID
        |   YCAK.SALE_TM,	                                          -- 销售时间
        |   YCAK.REV_TM,		                                        -- 运营时间
        |   YCBK.STORE_NM as OPER_NM,                               -- 运营商名称
        |   if (YCAK.PRVC is null, YCBK.PRVC,YCAK.PRVC) as PRVC,	    -- 机器所在省
        |   if (YCAK.CTY is null, YCBK.CTY,YCAK.CTY) as CTY,		      -- 机器所在市
        |   YCBK.AREA,				                                      -- 机器所在区域
        |   if (YCAK.ADDR_FMT is null, YCBK.ADDR, YCAK.ADDR_FMT) as ADDR, --机器详细地址
        |   YCBK.STORE_NM,	                                        -- 门店名称
        |   YCBK.TAG_NM as SCENCE_CATGY,                            -- 主场景名称
        |   YCBK.SUB_SCENE_CATGY_NM as SUB_SCENCE_CATGY,            -- 子场景分类名称
        |   YCBK.SUB_TAG_NM as SCENE ,                              -- 主场景分类名称
        |   YCBK.SUB_SCENE_NM as SUB_SCENE ,                        -- 子场景名称
        |   YCBK.BRND_NM as BRND,	                                  -- 主场景品牌
        |   YCBK.SUB_BRND_NM as SUB_BRND, 	                        -- 子场景品牌
        |   YCBK.PKG_NM as PRDCT_NM,                                -- 产品名称
        |   2 as PRDCT_TYP,	                                        -- 产品类型
        |   case when YCBK.PKG_NM = '联营版' then '联营'
        |     when YCBK.INV_RATE < 100 then '联营'
        |     else '卖断' end BUS_MODE,                              -- 运营模式
        |   YCBK.INV_RATE, 	                                        -- 投资人分成比例
        |   YCBK.AGE_RATE,	                                        -- 代理人、联盟人分成比例
        |   YCBK.COM_RATE,	                                        -- 公司分成比例
        |   YCBK.PAR_RATE,	                                        -- 合作方分成比例
        |   if (YCAK.STS is null, YCBK.IS_ACTV,YCAK.STS) as IS_ACTV,-- 是否激活
        |   YCBK.ACTV_TM,	                                          -- 激活时间
        |   if (YCAK.PAY_SW is null, YCBK.PAY_SW, YCAK.PAY_SW) as PAY_SW,  -- 是否开通移动支付
        |   YCBK.STORE_NM as PRTN_NM,	                              -- 代理人姓名，这里获取门店名称
        |   YCAK.CUR_LOGIN_TM	                                      -- 最近登录时间
        | from TEMP_YCAK_MAC_INFO as YCAK
        | left join TEMP_YCBK_MAC_INFO as YCBK ON YCAK.MID = YCBK.MID
      """.stripMargin).createTempView("result")

    // 8. 将以上结果保存到 Hive EDS层分区表
    sparkSession.sql(
      s"""
        | insert overwrite table tw_mac_baseinfo_d partition(data_dt=${currentDate}) select * from result
      """.stripMargin)

    // 9. 将以上结果写入到MySql中提供查询
    val props = new Properties()
    props.setProperty("user", mysqlUser)
    props.setProperty("password", mysqlPassword)
    props.setProperty("driver", "com.mysql.jdbc.Driver")
    sparkSession.sql(
      s"""
        | select ${currentDate} as data_dt, * from result
      """.stripMargin).write.mode(SaveMode.Overwrite).jdbc(mysqlUrl, "tm_mac_baseinfo", props)

    println("**** all finished ****")
  }

  /************ UDF 匿名函数 ************/
  // UDF 格式化时间
  val getFormatTime = ((time: String) => {
    if (time.length == 14) time
    else if (time.length == 8) time + "000000"
    else "19700101000000"
  })
  // UDF 格式化省份
  val getPrvcInfo = (prvc: String, addr: String) => {
    if (prvc == "null") {
      if (addr != null) getProvince(addr)
      else ""
    } else {
      prvc
    }
  }

  def getProvince(addr: String): String = {
    if (addr.contains("内蒙古自治区")) "内蒙古"
    else if (addr.contains("宁夏回族自治区")) "宁夏"
    else if (addr.contains("西藏自治区")) "西藏"
    else if (addr.contains("广西壮族自治区")) "广西"
    else if (addr.contains("新疆维吾尔自治区")) "新疆"
    else if (addr.contains("北京市")) "北京"
    else if (addr.contains("上海市")) "上海"
    else if (addr.contains("重庆市")) "重庆"
    else if (addr.contains("天津市")) "天津"
    else if (addr.contains("省")) addr.substring(0, addr.indexOf("省"))
    else ""
  }

  // UDF 格式化城市和区
  val getCityInfo = (city: String, addr: String) => {
    if (city == "null") {
      if (addr != null) getCity(addr)
      else ""
    }
    else ""
  }

  def getCity(addr: String): String = {
    if (StringUtils.containsAny(addr, Array("内蒙古自治区", "宁夏回族自治区", "西藏自治区", "广西壮族自治区", "新疆维吾尔自治区")))
      try {
        addr.substring(addr.indexOf("区") + 1, addr.indexOf("市"))
      } catch {
        case e: Exception =>
          val index = addr.indexOf("区")
          if (addr.substring(index + 1, addr.length).contains("区")) {
            addr.substring(index + 1, addr.indexOf("区", index + 1))
          } else {
            addr.substring(index + 1, addr.indexOf("州", index + 1))
          }
      }
    else if (StringUtils.containsAny(addr, Array("北京市", "上海市", "重庆市", "天津市"))) {
      val index = addr.indexOf("市")
      val s = addr.substring(index + 1, addr.length)
      if (s.contains("区")) {
        return addr.substring(index + 1, addr.indexOf("区"))
      } else if (s.contains("县")) {
        return addr.substring(index + 1, addr.indexOf("县"))
      }
      ""
    }
    else if (addr.contains("省")) {
      val index = addr.indexOf("省")
      val s = addr.substring(index + 1, addr.length)
      if (s.contains("市")) {
        return addr.substring(index + 1, addr.indexOf("市"))
      } else if (s.contains("州")) {
        return addr.substring(index + 1, s.indexOf("州"))
      }
      ""
    } else ""
  }
}
