package com.yw.recommend.program.profile

import com.yw.recommend.program.util.{DataUtils, HBaseUtil, PropertiesUtils, SparkSessionBase}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
 * 绘制用户画像
 * 通过用户喜欢的节目来给用户打标签
 */
object UserProfile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionBase.createSparkSession()
    import spark.implicits._

    val TOTAL_SCORE = 10

    val itemID2ActionRDD = spark.table("program.user_action")
      // 方便测试，只取1000条
      .limit(1000)
      .map(row => {
        val userID = row.getAs[String]("sn")
        val itemID = row.getAs[Int]("item_id")
        val duration = row.getAs[Long]("duration")
        val time = row.getAs[String]("ts")
        (itemID, (userID, duration, time))
      }).rdd

    val itemID2KeyWordRDD = spark.table("program.item_keyword").map(row => {
      val itemID = row.getAs[Int]("item_id")
      val keywords = row.getAs[Seq[String]]("keyword")
      (itemID, keywords)
    }).rdd

    val userID2InfoRDD = spark.table("program.user_info").map(row => {
      val userID = row.getAs[String]("sn")
      val province = row.getAs[String]("province")
      val city = row.getAs[String]("city")
      (userID, (province, city))
    }).rdd


    /**
     * 通过用户喜欢的节目来为用户打标签，同时还要通过duration停留时间为标签打分值
     * 打分值：
     * （1）根据停留的时长与总时长的比例 打分值，满分10分
     * （2）添加时间衰减因子  时间衰减:1/(log(t)+1)
     */
    // 获取每一个节目的总时长
    val itemID2LengthMap = spark.table("program.item_info").map(row => {
      val itemID = row.getAs[Long]("id").toInt
      val length = row.getAs[Long]("length")
      (itemID, length)
    }).collect().toMap

    // 由于节目信息数据量并不是很大，完全可以放入在广播变量中保存
    val itemID2LengthMapBroad = spark.sparkContext.broadcast(itemID2LengthMap)

    /* *
     * 调优点：
     * 如果存在某一些黑客用户 疯狂点击视频，势必会造成在数据计算的过程，产生数据倾斜问题
     * （1）从源头上根据duration来筛选
     * （2）在join计算的通过技术手段解决数据倾斜问题
     * */
    val userID2LabelRDD = itemID2ActionRDD.join(itemID2KeyWordRDD)
      .map(item => {
        val itemID = item._1
        val userID = item._2._1._1
        val duration = item._2._1._2
        val time = item._2._1._3
        val keywords = item._2._2
        val itemID2LengthMap = itemID2LengthMapBroad.value
        val length = itemID2LengthMap(itemID)
        // TODO: 数据需要修改
        val score = if (duration < length) {
          val durationScale = (duration * 1.0) / length
          val scalaScore = durationScale * TOTAL_SCORE
          val days = DataUtils.getDayDiff(time)
          // 衰减系数计算公式：1/(log(t)+1)
          val attenCoeff = 1 / (math.log(days) + 1)
          attenCoeff * scalaScore
        } else 0.0
        ((itemID, userID), (duration, time, keywords, score))
      }).groupByKey()
      .map(item => {
        val (itemID, userID) = item._1
        var time = ""
        var keywords = new ListBuffer[String]()
        var score = 0.0
        for (elem <- item._2.iterator) {
          if ("".equals(time)) time = elem._2
          else time = DataUtils.getMaxDate(elem._2, time)
          if (score < elem._4) score = elem._4
          if (keywords.isEmpty) keywords.++=(elem._3)
        }
        (userID, (itemID, time, keywords, score))
      })

    /**
     * 补全用户画像，补充用户基础信息 并且存储的到HBase数据库
     */
    val tmpRDD = userID2LabelRDD.join(userID2InfoRDD)
      .map(data => {
        val userID = data._1
        val itemID = data._2._1._1
        val time = data._2._1._2
        val keywords = data._2._1._3
        val score = data._2._1._4
        val province = data._2._2._1
        val city = data._2._2._2
        ((userID), (itemID, time, keywords, score, province, city))
      }).groupByKey()
      .foreachPartition(partition => {
        for (row <- partition) {
          val userID = row._1
          val profiles = row._2
          saveUserProfileToHBase(userID, profiles)
        }
      })

    /**
     * 补全用户画像，补充用户基础信息
     *
     *
     * val tmpRDD = userID2LabelRDD.join(userID2InfoRDD)
     * .map(data => {
     * val userID = data._1
     * val itemID = data._2._1._1
     * val time = data._2._1._3
     * val keywords = data._2._1._4
     * val score = data._2._1._5
     * val province = data._2._2._1
     * val city = data._2._2._2
     * ((userID, itemID), (time, keywords, score, province, city))
     * }).groupByKey()
     *
     * tmpRDD.map(item => {
     *
     * val (userID, itemID) = item._1
     * val iterator = item._2.iterator
     * var time = ""
     * var keywords = new ListBuffer[String]()
     * var score = 0.0
     * var province = ""
     * var city = ""
     * while (iterator.hasNext) {
     * val item = iterator.next()
     * //              (time, keywords, score, province, city))
     * if ("".equals(time)) time = item._1
     * else time = DataUtils.getMaxDate(item._1, time)
     * if (score < item._3) score = item._3
     * if (keywords.length == 0) keywords.++=(item._2)
     * if ("".equals(province)) province = item._4
     * if ("".equals(city)) city = item._5
     * ((userID, (itemID, keywords, score, province, city)))
     * }
     * }).groupByKey()
     */
    spark.close()
  }

  /**
   * 用户画像数据插入到HBase数据库中
   * ((userID), (itemID, time, keywords, score, province, city))
   * create 'user_profile', {NAME => 'label', VERSIONS => 1}, {NAME => 'info', VERSIONS => 1}
   * itemID: Int, keywords: ListBuffer[String], score: Double, province: String, city: String
   */
  def saveUserProfileToHBase(userID: String, profiles: Iterable[(Int, String, ListBuffer[String], Double, String, String)]): Unit = {
    val tableName = PropertiesUtils.getProp("user.profile.hbase.table")
    val htable = HBaseUtil.getUserProfileTable(tableName)
    val put = new Put(Bytes.toBytes(userID))
    var province = ""
    var city = ""
    var itemID = ""
    var score = 0.0
    for (elem <- profiles) {
      itemID = elem._1.toString
      val keyWord = elem._3.mkString("\t")
      score = elem._4
      province = elem._5
      city = elem._6
      put.addColumn(Bytes.toBytes("label"), Bytes.toBytes("itemID:" + itemID), Bytes.toBytes("keyWord:" + keyWord + "|score:" + score))
    }
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("province"), Bytes.toBytes(province))
//    put.addColumn(Bytes.toBytes("label"), Bytes.toBytes("score"), Bytes.toBytes(itemID + ":" + score))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("city"), Bytes.toBytes(city))
    htable.put(put)
  }
}
