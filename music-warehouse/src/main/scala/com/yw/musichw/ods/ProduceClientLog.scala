package com.yw.musichw.ods

import com.alibaba.fastjson.{JSON, JSONObject}
import com.yw.musichw.base.PairRDDMultipleTextOutputFormat
import com.yw.musichw.util.ConfigUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 读取 运维人员上传到服务器上的客户端日志，进行解析，并加载到 ODS层 的表中
  *
  * !!!注意：运行此类需要指定参数
  * 参数一：当前日期，格式 20221230
  * 参数二：日志保存路径，例如 hdfs://node01/logdata/currentday_clientlog.tar.gz
  * 本地运行，直接指定 ./music-warehouse/data/currentday_clientlog.tar.gz
  *
  * @author yangwei
  */
object ProduceClientLog {
  private val localRun: Boolean = ConfigUtils.LOCAL_RUN
  private val hiveMetaStoreUris = ConfigUtils.HIVE_METASTORE_URIS
  private val hiveDatabase = ConfigUtils.HIVE_DATABASE
  private val hdfsClientLogPath = ConfigUtils.HDFS_CLIENT_LOG_PATH

  private var sparkSession: SparkSession = _
  private var sc: SparkContext = _
  private var clientLogInfos: RDD[String] = _

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println(s"需要指定 数据日期")
      System.exit(1)
    }
    val logDate = args(0) // 日期格式：年月日 20221230

    if (localRun) {
      sparkSession = SparkSession.builder()
        .master("local")
        .appName(this.getClass.getSimpleName)
        .config("hive.metastore.uris", hiveMetaStoreUris).enableHiveSupport().getOrCreate()
      sc = sparkSession.sparkContext
      clientLogInfos = sc.textFile("/Volumes/F/MyGitHub/bigdata/music-warehouse/data/currentday_clientlog.tar.gz")
    } else {
      sparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
        .enableHiveSupport().getOrCreate()
      sc = sparkSession.sparkContext
      clientLogInfos = sc.textFile(s"${hdfsClientLogPath}/currentday_clientlog.tar.gz")
    }

    // 组织 K,V 格式数据: (客户端请求类型，对应的info信息)
    val tableNameAndInfos = clientLogInfos.map(line => line.split("&"))
      .filter(item => item.length == 6)
      .map(line => (line(2), line(3)))
    // 获取当日出现的所有的"请求类型"
    val allTableNames = tableNameAndInfos.keys.distinct().collect()
    // 转换数据，将数据分别以表名方式存储在某个路径中
    tableNameAndInfos.map(tp => {
      val tableName = tp._1 // 客户端请求类型
      val tableInfos = tp._2 // 请求的 json string 数据
      if ("MINIK_CLIENT_SONG_PLAY_OPERATE_REQ".equals(tableName)) {
        val jsonObject: JSONObject = JSON.parseObject(tableInfos)
        val songId = jsonObject.getString("songid") // 歌曲ID
        val mid = jsonObject.getString("mid") // 机器ID
        val optrateType = jsonObject.getString("optrate_type") // 0:点歌, 1:切歌,2:歌曲开始播放,3:歌曲播放完成,4:录音试听开始,5:录音试听切歌,6:录音试听完成
        val uid = jsonObject.getString("uid") // 用户ID（无用户则为0）
        val consumeType = jsonObject.getString("consume_type") // 消费类型：0免费；1付费
        val durTime = jsonObject.getString("dur_time") // 总时长，单位秒（operate_type:0时此值为0）
        val sessionId = jsonObject.getString("session_id") // session ID
        val songName = jsonObject.getString("songname") // 歌曲名
        val pkgId = jsonObject.getString("pkg_id") // 套餐ID类型
        val orderId = jsonObject.getString("order_id") // 订单号
        (tableName, songId + "\t" + mid + "\t" + optrateType + "\t" + uid + "\t" + consumeType + "\t" + durTime + "\t" + sessionId + "\t" + songName + "\t" + pkgId + "\t" + orderId)
      } else {
        // 将其他表的infos 信息直接以json格式保存到目录中
        tp
      }
    }).saveAsHadoopFile(
      s"${hdfsClientLogPath}/all_client_tables/${logDate}",
      classOf[String],
      classOf[String],
      classOf[PairRDDMultipleTextOutputFormat]
    )

    /**
      * 在Hive中创建ODS层的 TO_CLIENT_SONG_PLAY_OPERATE_REQ_D 表
      */
    sparkSession.sql(s"use $hiveDatabase")
    sparkSession.sql(
      """
        |CREATE EXTERNAL TABLE IF NOT EXISTS `TO_CLIENT_SONG_PLAY_OPERATE_REQ_D`(
        | `SONGID` string,        --歌曲ID
        | `MID` string,           --机器ID
        | `OPTRATE_TYPE` string,  --操作类型
        | `UID` string,           --用户ID
        | `CONSUME_TYPE` string,  --消费类型
        | `DUR_TIME` string,      --时长
        | `SESSION_ID` string,    --sessionID
        | `SONGNAME` string,      --歌曲名称
        | `PKG_ID` string,        --套餐ID
        | `ORDER_ID` string       --订单ID
        |)
        |partitioned by (data_dt string)
        |ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
        |LOCATION 'hdfs://node01/user/hive/warehouse/music.db/TO_CLIENT_SONG_PLAY_OPERATE_REQ_D'
      """.stripMargin)

    sparkSession.sql(
      s"""
         | load data inpath
         | '${hdfsClientLogPath}/all_client_tables/${logDate}/MINIK_CLIENT_SONG_PLAY_OPERATE_REQ'
         | into table TO_CLIENT_SONG_PLAY_OPERATE_REQ_D partition (data_dt='${logDate}')
      """.stripMargin)

    println("**** all finished ****")
  }
}
