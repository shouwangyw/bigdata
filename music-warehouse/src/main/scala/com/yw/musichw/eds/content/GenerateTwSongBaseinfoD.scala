package com.yw.musichw.eds.content

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.yw.musichw.util.{ConfigUtils, DateUtils}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * 生成TW层 TW_SONG_BASEINFO_D 数据表
  * 主要是读取Hive中的ODS层 TO_SONG_INFO_D 表生成 TW层 TW_SONG_BASEINFO_D表
  *
  * @author yangwei
  */
object GenerateTwSongBaseinfoD {
  val localRun: Boolean = ConfigUtils.LOCAL_RUN
  val hiveMetaStoreUris = ConfigUtils.HIVE_METASTORE_URIS
  val hiveDatabase = ConfigUtils.HIVE_DATABASE
  var sparkSession: SparkSession = _

  /* 定义使用到的 UDF 函数对应的方法 */
  /**
    * 获取专辑名称
    */
  val getAlbumName: String => String = (albumInfo: String) => {
    var albumName = ""
    try {
      val jsonArray: JSONArray = JSON.parseArray(albumInfo)
      albumName = jsonArray.getJSONObject(0).getString("name")
    } catch {
      case e: Exception => {
        if (albumInfo.contains("《") && albumInfo.contains("》")) {
          albumName = albumInfo.substring(albumInfo.indexOf('《'), albumInfo.indexOf('》') + 1)
        } else {
          albumName = "暂无专辑"
        }
      }
    }
    albumName
  }

  /**
    * 获取发行时间
    */
  val getPostTime: String => String = (postTime: String) => {
    DateUtils.formatDate(postTime)
  }

  /**
    * 获取歌手信息
    */
  val getSingerInfo: (String, String, String) => String = (singerInfos: String, singer: String, nameOrId: String) => {
    var singerNameOrSingerId = ""
    try {
      val jsonArray: JSONArray = JSON.parseArray(singerInfos)
      if ("singer1".equals(singer) && "name".equals(nameOrId) && jsonArray.size() > 0) {
        singerNameOrSingerId = jsonArray.getJSONObject(0).getString("name")
      } else if ("singer1".equals(singer) && "id".equals(nameOrId) && jsonArray.size() > 0) {
        singerNameOrSingerId = jsonArray.getJSONObject(0).getString("id")
      } else if ("singer2".equals(singer) && "name".equals(nameOrId) && jsonArray.size() > 1) {
        singerNameOrSingerId = jsonArray.getJSONObject(1).getString("name")
      } else if ("singer2".equals(singer) && "id".equals(nameOrId) && jsonArray.size() > 1) {
        singerNameOrSingerId = jsonArray.getJSONObject(1).getString("id")
      }
    } catch {
      case e: Exception => {
        singerNameOrSingerId
      }
    }
    singerNameOrSingerId
  }

  /**
    * 获取授权公司
    */
  val getAuthCompany: String => String = (authCompanyInfo: String) => {
    var authCompanyName = "乐心曲库"
    try {
      val jsonObject: JSONObject = JSON.parseObject(authCompanyInfo)
      authCompanyName = jsonObject.getString("name")
    } catch {
      case e: Exception => {
        authCompanyName
      }
    }
    authCompanyName
  }

  /**
    * 获取产品类型
    */
  val getProductType: (String => ListBuffer[Int]) = (productTypeInfo: String) => {
    val list = new ListBuffer[Int]()
    if (!"".equals(productTypeInfo.trim)) {
      val strings = productTypeInfo.stripPrefix("[").stripSuffix("]").split(",")
      strings.foreach(t => list.append(t.toDouble.toInt))
    }
    list
  }

  def main(args: Array[String]): Unit = {
    if (localRun) { // 本地运行
      sparkSession = SparkSession.builder().master("local")
        .config("hive.metastore.uris", hiveMetaStoreUris)
        .enableHiveSupport().getOrCreate()
    } else { // 集群运行
      sparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    }

    import org.apache.spark.sql.functions._ //导入函数，可以使用 udf、col 方法

    // 构建转换数据的 UDF
    val udfGetAlbumName = udf(getAlbumName)
    val udfGetPostTime = udf(getPostTime)
    val udfGetSingerInfo = udf(getSingerInfo)
    val udfGetAuthCompany = udf(getAuthCompany)
    val udfGetProductType = udf(getProductType)

    sparkSession.sql(s"use $hiveDatabase")
    sparkSession.table("TO_SONG_INFO_D")
      .withColumn("ALBUM", udfGetAlbumName(col("ALBUM")))
      .withColumn("POST_TIME", udfGetPostTime(col("POST_TIME")))
      .withColumn("SINGER1", udfGetSingerInfo(col("SINGER_INFO"), lit("singer1"), lit("name")))
      .withColumn("SINGER1ID", udfGetSingerInfo(col("SINGER_INFO"), lit("singer1"), lit("id")))
      .withColumn("SINGER2", udfGetSingerInfo(col("SINGER_INFO"), lit("singer2"), lit("name")))
      .withColumn("SINGER2ID", udfGetSingerInfo(col("SINGER_INFO"), lit("singer2"), lit("id")))
      .withColumn("AUTH_CO", udfGetAuthCompany(col("AUTH_CO")))
      .withColumn("PRDCT_TYPE", udfGetProductType(col("PRDCT_TYPE")))
      .createTempView("TEMP_TO_SONG_INFO_D")

    /*
    清洗数据，将结果保存到 Hive TW_SONG_BASEINFO_D 表中
     */
    sparkSession.sql(
      """
        | select NBR,
        |       nvl(NAME,OTHER_NAME) as NAME,
        |       SOURCE,
        |       ALBUM,
        |       PRDCT,
        |       LANG,
        |       VIDEO_FORMAT,
        |       DUR,
        |       SINGER1,
        |       SINGER2,
        |       SINGER1ID,
        |       SINGER2ID,
        |       0 as MAC_TIME,
        |       POST_TIME,
        |       PINYIN_FST,
        |       PINYIN,
        |       SING_TYPE,
        |       ORI_SINGER,
        |       LYRICIST,
        |       COMPOSER,
        |       BPM_VAL,
        |       STAR_LEVEL,
        |       VIDEO_QLTY,
        |       VIDEO_MK,
        |       VIDEO_FTUR,
        |       LYRIC_FTUR,
        |       IMG_QLTY,
        |       SUBTITLES_TYPE,
        |       AUDIO_FMT,
        |       ORI_SOUND_QLTY,
        |       ORI_TRK,
        |       ORI_TRK_VOL,
        |       ACC_VER,
        |       ACC_QLTY,
        |       ACC_TRK_VOL,
        |       ACC_TRK,
        |       WIDTH,
        |       HEIGHT,
        |       VIDEO_RSVL,
        |       SONG_VER,
        |       AUTH_CO,
        |       STATE,
        |       case when size(PRDCT_TYPE) = 0 then NULL else PRDCT_TYPE  end as PRDCT_TYPE
        |    from TEMP_TO_SONG_INFO_D
        |    where NBR != ''
      """.stripMargin).write.format("Hive").mode(SaveMode.Overwrite).saveAsTable("TW_SONG_BASEINFO_D")

    println("**** all finished ****")
  }
}
