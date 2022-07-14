package com.yw.spark.example.core.cases

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkConf, SparkContext, TaskContext}

/**
  * 实现自定义RDD
  *
  * @author yangwei
  */
case class SalesRecord(val transactionId: String,
                       val customerId: String,
                       val itemId: String,
                       val itemValue: Double) extends Serializable

/**
  * 定义增强函数
  */
class CustomFunctions(rdd: RDD[SalesRecord]) {
  def changeDatas: RDD[Double] = rdd.map(x => x.itemValue)

  def getTotalValue: Double = rdd.map(x => x.itemValue).sum()

  def discount(discountPercentage: Double) = new CustomRDD(rdd, discountPercentage)
}
object CustomFunctions {
  implicit def addIteblogCustomFunctions(rdd: RDD[SalesRecord]) = new CustomFunctions(rdd)
}
/**
  * 自定义定义RDD
  */
class CustomRDD(prev: RDD[SalesRecord], discountPercentage: Double) extends RDD[SalesRecord](prev) {
  override def compute(split: Partition, context: TaskContext): Iterator[SalesRecord] = {
    firstParent[SalesRecord].iterator(split, context).map(salesRecord => {
      val discount = salesRecord.itemValue * discountPercentage
      // 样例类，不需要new
      SalesRecord(salesRecord.transactionId, salesRecord.customerId, salesRecord.itemId, discount)
    })
  }

  override protected def getPartitions: Array[Partition] = {
    firstParent[SalesRecord].partitions
  }
}

object Case08_CustomRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val dataRDD = sc.textFile(this.getClass.getClassLoader.getResource("sales.txt").getPath)
    val salesRecordRDD: RDD[SalesRecord] =  dataRDD.map(row => {
      val colValues = row.split(",")
      SalesRecord(colValues(0), colValues(1), colValues(2), colValues(3).toDouble)
    })

    import com.yw.spark.example.core.cases.CustomFunctions._

    // 总金额
    println("Spark RDD API: " + salesRecordRDD.map(_.itemValue).sum)
    // output: Spark RDD API: 892.0

    // 通过隐式转换的方法，增加rdd的transformation算子
    // 需求一：获得item金额
    val moneyRDD: RDD[Double] = salesRecordRDD.changeDatas
    println("customer RDD  API: " + moneyRDD.collect().toBuffer)
    // output: customer RDD  API: ArrayBuffer(128.0, 135.0, 147.0, 196.0, 178.0, 108.0)

    // 需求二：给rdd增加action算子，获得总金额
    val totalResult: Double = salesRecordRDD.getTotalValue
    println("total_result: " + totalResult)
    // output: total_result: 892.0

    // 需求三：自定义RDD，将RDD转换成为新的RDD
    val resultCountRDD: CustomRDD = salesRecordRDD.discount(0.8)

    println(resultCountRDD.collect().toBuffer)
    // output: ArrayBuffer(SalesRecord(1,userid1,itemid1,102.4), SalesRecord(2,userid2,itemid2,108.0), SalesRecord(3,userid3,itemid3,117.60000000000001), SalesRecord(4,userid4,itemid4,156.8), SalesRecord(5,userid5,itemid5,142.4), SalesRecord(6,userid6,itemid6,86.4))

    sc.stop()
  }
}
