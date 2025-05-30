package com.yw.flink.example.scalacases.case14_tableAndStreamAndSql

import com.yw.flink.example.StationLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{Over, OverWindowedTable, Schema, Table}

/**
  * Flink Table API - Over 开窗函数
  * 案例：读取基站日志数据，Over开窗函数统计每条数据最近2s该基站通话总时长
  */
object Case15_OverWindowBaseTime {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //自动推进watermark
    tableEnv.getConfig.set("table.exec.source.idle-timeout", "5000")

    val ds: DataStream[StationLog] = env.socketTextStream("node5", 9999)
      .map(line => {
        val split: Array[String] = line.split(",")
        StationLog(split(0), split(1), split(2), split(3), split(4).toLong, split(5).toLong)
      })

    val table: Table = tableEnv.fromDataStream(ds, Schema.newBuilder()
      .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(callTime,3)")
      .watermark("rowtime", "rowtime - INTERVAL '2' SECONDS")
      .build())

    val window: OverWindowedTable = table.window(
      Over partitionBy $"sid" orderBy $"rowtime"
        preceding 2.seconds() following CURRENT_RANGE as "w"
    )

    val result: Table = window.select(
      $"sid",
      $"duration",
      $"callTime",
      $"duration".sum over $"w" as "sum_duration"
    )

    result.execute().print()
  }
}
