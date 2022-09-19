package com.yw.musichw.eds.machine

import com.alibaba.fastjson.{JSON, JSONObject}
import com.yw.musichw.util.{ConfigUtils, DateUtils, StringUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scalaj.http.{Http, HttpOptions, HttpResponse}

import scala.collection.mutable.ListBuffer

/**
  * 由 ODS 层 TO_YCAK_USR_LOC_D 用户位置记录日增量表，统计得到 TW_MAC_LOC_D(机器位置信息日统计表)
  * 这里每天都会基于增量数据进行统计：TW_MAC_LOC_D(机器位置信息日统计表)，每次统计出来数据都是全量的机器信息位置。
  *
  * 实现思路：
  *   1. 根据 TO_YCAK_USR_LOC_D 用户位置记录表 过去30天的数据，进行机器位置统计，找出过去30天中出现位置最多的每个机器对应的一条位置记录
  *   2. 根据高德地图api获取机器位置
  *   3. 与之前统计的 TW_MAC_LOC_D(机器位置信息日统计表) 做合并，统计全量的机器位置信息
  *
  * @author yangwei
  */
object GenerateTwMacLocD {
  val localRun: Boolean = ConfigUtils.LOCAL_RUN
  val hiveMetaStoreUris = ConfigUtils.HIVE_METASTORE_URIS
  val hiveDatabase = ConfigUtils.HIVE_DATABASE
  var sparkSession: SparkSession = _

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
    sparkSession.sparkContext.setLogLevel("Error")

    /**
      * 统计 TO_YCAK_USR_LOC_D 表中 过去30天机器最新的位置
      */
    // 根据当前输入的日期，获取过去30天的日期
    val pre30Date = DateUtils.getCurrentDatePreDate(currentDate, 30)
    val pre30DaysDF = sparkSession.sql(
      s"""
         | select
         |    UID,    -- 用户ID
         |    MID,    -- 机器ID
         |    LAT,    -- 维度
         |    LNG     -- 经度
         | from TO_YCAK_USR_LOC_D
         | where data_dt between ${pre30Date} and ${currentDate}
       """.stripMargin)
    pre30DaysDF.distinct() // 重复用户上报的机器位置不计数
      .groupBy("MID", "LAT", "LNG")
      .count()
      .withColumnRenamed("LAT", "X") // 维度
      .withColumnRenamed("LNG", "Y") // 经度
      .withColumnRenamed("count", "CNT")
      .createTempView("TEMP_PRE30_MAC_LOC_INFO")

    // 根据不同用户上报数据，筛选出所有机器位置中每台机器出现次数最多的机器位置
    val macLocDF: DataFrame = sparkSession.sql(
      """
        | select
        |   MID,     -- 机器ID
        |   X,       -- 维度
        |   Y,       -- 经度
        |   CNT,     -- 出现次数
        |   row_number() over(partition by MID order by CNT desc) as RANK
        | from TEMP_PRE30_MAC_LOC_INFO
      """.stripMargin).filter("x != '' and y != '' and RANK = 1")
    val rowRDD: RDD[Row] = macLocDF.rdd.mapPartitions(it => {
      val detailLocalInfo = new ListBuffer[Row]
      val list: List[Row] = it.toList
      // 获取 list 长度
      val length = list.size
      // 这里高德地图api调用限制每次并发10个，所以times是统计要进行几次调用高德地图
      var times = 0
      if (length % 10 != 0) {
        times = length / 10 + 1
      } else {
        times = length / 10
      }
      for (i <- 0 until times) {
        // slice(m,n)方法，提取集合中第m个元素一直到第n-1个元素
        val currentRows = list.slice(i * 10, i * 10 + 10)
        val rows: ListBuffer[Row] = getLocInfoFromGeoApi(currentRows)
        detailLocalInfo.++=(rows)
      }
      detailLocalInfo.iterator
    })
    val schema = StructType(Array[StructField](
      StructField("MID", IntegerType),
      StructField("X", StringType),
      StructField("Y", StringType),
      StructField("CNT", IntegerType),
      StructField("ADDR", StringType),
      StructField("PRVC", StringType),
      StructField("CIT", StringType),
      StructField("CIT_CD", StringType),
      StructField("DISTRICT", StringType),
      StructField("AD_CD", StringType),
      StructField("TOWN_SHIP", StringType),
      StructField("TOWN_CD", StringType),
      StructField("NB_NM", StringType),
      StructField("NB_TP", StringType),
      StructField("BD_NM", StringType),
      StructField("BD_TP", StringType),
      StructField("STREET", StringType),
      StructField("STREET_NB", StringType),
      StructField("STREET_LOC", StringType),
      StructField("STREET_DRCTION", StringType),
      StructField("STREET_DSTANCE", StringType),
      StructField("BUS_INFO", StringType)
    ))

    import org.apache.spark.sql.functions._
    // 当天统计的过去30天中机器的位置信息
    val pre30DaysMacLocInfos: DataFrame = sparkSession.createDataFrame(rowRDD, schema)

    /**
      * 获取昨天 TW_MAC_LOC_D 机器位置信息日统计表 中统计的所有机器的位置信息
      * 并与今天统计的过去30天的机器位置信息做差集，找出30天前的机器位置信息，然后与今天统计的过去30天的机器位置信息做交集
      * 得到目前位置，所有机器的位置信息
      */
    val pre1Date = DateUtils.getCurrentDatePreDate(currentDate, 1)
    val pre1DateMacLocInfo = sparkSession.table("TW_MAC_LOC_D")
      .where(s"data_dt = ${pre1Date}")
    // 取二者差集，前面与后面不同的数据
    val diffMid = pre1DateMacLocInfo.select("MID").except(pre30DaysMacLocInfos.select("MID"))
    // 按照mid 左连接 关联 pre1DateMacLocInfo 获取30天前的机器详细信息然后与当前计算的 最近30天机器信息做union
    val allMacLocInfos = diffMid.join(pre1DateMacLocInfo, Seq("mid"), "left")
      .drop(col("data_dt")).union(pre30DaysMacLocInfos)
    allMacLocInfos.createTempView("TEMP_ALL_MAC_LOC_INFO")

    sparkSession.sql(
      s"""
         | insert overwrite table tw_mac_loc_d partition(data_dt = ${currentDate}) select * from TEMP_ALL_MAC_LOC_INFO
       """.stripMargin)

    println("**** all finished ****")
  }

  /**
    * 从高德API中获取对应的机器位置信息
    */
  def getLocInfoFromGeoApi(rows: List[Row]): ListBuffer[Row] = {
    val results = new ListBuffer[Row]()
    // 获取 rows 中的每条数据的经纬度，并按照 “|” 拼接成字符串
    var concatYX = ""
    for (i <- 0 until rows.size) {
      val X = rows(i).getAs[String]("X") // 维度
      val Y = rows(i).getAs[String]("Y") // 经度
      concatYX += Y + "," + X + "|"
    }
    // 调用高德api，根据经纬度获取对应的地址
    val res: HttpResponse[String] = Http("https://restapi.amap.com/v3/geocode/regeo")
      .param("key", "344bff6e68fdf2c56039a2bb8e4a36c6")
      .param("location", concatYX.substring(0, concatYX.length - 1))
      .param("batch", "true")
      .option(HttpOptions.readTimeout(10000)) // 获取数据延迟 10s
      .asString

    val jsonInfo: JSONObject = JSON.parseObject(res.body.toString)
    val geoCodes = JSON.parseArray(jsonInfo.getString("regeocodes"))
    val retLocLen = geoCodes.size() // 结果中返回的地址个数
    if ("10000".equals(jsonInfo.getString("infocode")) && rows.size == retLocLen) {
      // 从返回的json中获取详细地址，对从高德API中查询的数据进行整理，转换成Row类型的数据返回
      for (i <- 0 until rows.length) {
        val mid = rows(i).getAs[String]("MID").toInt
        val x = rows(i).getAs[String]("X") // 维度
        val y = rows(i).getAs[String]("Y") // 经度
        val cnt = rows(i).getAs[Long]("CNT").toInt // 出现次数
        val currentGeoCode = geoCodes.getJSONObject(i)
        val address = StringUtils.checkString(currentGeoCode.getString("formatted_address"))

        val addressComponent = currentGeoCode.getJSONObject("addressComponent")
        val province = StringUtils.checkString(addressComponent.getString("province"))
        val city = StringUtils.checkString(addressComponent.getString("city"))
        val citycode = StringUtils.checkString(addressComponent.getString("citycode"))
        val district = StringUtils.checkString(addressComponent.getString("district"))
        val adcode = StringUtils.checkString(addressComponent.getString("adcode"))
        val township = StringUtils.checkString(addressComponent.getString("township"))
        val towncode = StringUtils.checkString(addressComponent.getString("towncode"))

        val neighborhood = addressComponent.getJSONObject("neighborhood")
        val neighborhoodName = StringUtils.checkString(neighborhood.getString("name"))
        val neighborhoodType = StringUtils.checkString(neighborhood.getString("type"))

        val building = addressComponent.getJSONObject("building")
        val buildingName = StringUtils.checkString(building.getString("name"))
        val buildingType = StringUtils.checkString(building.getString("type"))

        val streetNumber = addressComponent.getJSONObject("streetNumber")
        val street = StringUtils.checkString(streetNumber.getString("street"))
        val number = StringUtils.checkString(streetNumber.getString("number"))
        val location = StringUtils.checkString(streetNumber.getString("location"))
        val direction = StringUtils.checkString(streetNumber.getString("direction"))
        val distance = StringUtils.checkString(streetNumber.getString("distance"))

        val businessAreas = addressComponent.getString("businessAreas")

        results.append(Row(mid, x, y, cnt, address, province, city, citycode, district, adcode, township, towncode,
          neighborhoodName, neighborhoodType, buildingName, buildingType, street, number, location, direction, distance, businessAreas))
      }
    }
    results
  }
}
