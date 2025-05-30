package com.yw.flink.example.scalacases.case14_tableAndStreamAndSql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Expressions.call
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, Schema, Table}
import org.apache.flink.table.functions.TemporalTableFunction

/**
  * Flink Table API - 创建查询时态表数据
  */
object Case19_TableApiTemporalJoin {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //watermark自动推进
    tableEnv.getConfig.set("table.exec.source.idle-timeout", "5000")

    //socket - 8888 ：p_002,1000
    val leftInfo: DataStream[(String, Long)] = env.socketTextStream("node5", 8888)
      .map(line => {
        val split: Array[String] = line.split(",")
        (split(0), split(1).toLong)
      })

    val leftTable: Table = tableEnv.fromDataStream(leftInfo,
      Schema.newBuilder()
        .column("_1", DataTypes.STRING())
        .column("_2", DataTypes.BIGINT())
        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(_2,3)")
        .watermark("rowtime", "rowtime - INTERVAL '5' SECONDS")
        .build()).as("left_product_id", "left_dt", "left_rowtime")

    //socket - 9999 ：1000,p_001,电脑,3.0
    val rightInfo: DataStream[(Long, String, String, Double)] = env.socketTextStream("node5", 9999)
      .map(line => {
        val split: Array[String] = line.split(",")
        (split(0).toLong, split(1), split(2), split(3).toDouble)
      })

    val rightTable: Table = tableEnv.fromDataStream(rightInfo,
      Schema.newBuilder().column("_1", DataTypes.BIGINT())
        .column("_2", DataTypes.STRING())
        .column("_3", DataTypes.STRING())
        .column("_4", DataTypes.DOUBLE())
        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(_1,3)")
        .watermark("rowtime", "rowtime - INTERVAL '5' SECONDS")
        .build()).as("right_update_time", "right_product_id", "right_product_name", "right_price",
      "right_rowtime")

    //创建时态表函数
    val function: TemporalTableFunction = rightTable.createTemporalTableFunction($"right_rowtime", $"right_product_id")

    //注册时态表函数
    tableEnv.createTemporarySystemFunction("temporalTableFunction", function)


    //通过时态表函数查询时态数据
    val result: Table = leftTable.joinLateral(
      call("temporalTableFunction", $"left_rowtime"),
      $"left_product_id" === $"right_product_id"
    ).select(
      $"left_product_id",
      $"left_dt",
      $"right_product_name",
      $"right_price"
    )

    result.execute().print()
  }
}
