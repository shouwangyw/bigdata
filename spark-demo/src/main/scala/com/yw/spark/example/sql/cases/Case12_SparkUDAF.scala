package com.yw.spark.example.sql.cases

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  *
  * @author yangwei
  */
object Case12_SparkUDAF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.format("csv")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .option("header", "true")
      .option("multiLine", "true")
      .load("/Volumes/F/MyGitHub/bigdata/spark-demo/src/main/resources/深圳链家二手房成交明细.csv")

    df.createOrReplaceTempView("house_sale")

    spark.sql("select floor from house_sale limit 30").show()
    spark.udf.register("udaf", new MyAverage)
    spark.sql("select floor, udaf(house_sale_money) from house_sale group by floor").show()
    df.printSchema()

    spark.stop()
  }
}
class MyAverage extends UserDefinedAggregateFunction {
  // 聚合函数输入函数的数据类型
  override def inputSchema: StructType = StructType(StructField("floor", DoubleType) :: Nil)

  // 聚合缓冲区中值的数据类型
  override def bufferSchema: StructType = {
    StructType(StructField("sum", DoubleType) :: StructField("count", LongType) :: Nil)
  }

  // 返回值类型
  override def dataType: DataType = DoubleType

  // 对于相同输入是否一直返回相同的输出
  override def deterministic: Boolean = true

  // 初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 用于存储不同类型的楼房的总成交额
    buffer(0) = 0D
    // 用于存储不同类型的楼房的总个数
    buffer(1) = 0L
  }

  // 相同Execute间的数据合并(分区内聚合)
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getDouble(0) + input.getDouble(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  // 不同Execute间的数据合并(分区外聚合)
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 计算最终结果
  override def evaluate(buffer: Row): Any = buffer.getDouble(0) / buffer.getLong(1)
}
