package com.yw.flink.example.scalacases.case14_tableAndStreamAndSql

import com.yw.flink.example.StationLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api._

/**
  * Flink Table API - 窗口使用
  * 案例：读取socket基站日志数据，每隔5秒设置滚动窗口统计每个基站的通话时长
  */
object Case20_Window {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    tableEnv.getConfig.set("table.exec.source.idle-timeout", "5000")

    val ds: DataStream[StationLog] = env.socketTextStream("node5", 9999)
      .map(line => {
        val split: Array[String] = line.split(",")
        StationLog(split(0), split(1), split(2), split(3), split(4).toLong, split(5).toLong)
      })

    val table: Table = tableEnv.fromDataStream(ds,
      Schema.newBuilder()
        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(callTime,3)")
        .watermark("rowtime", "rowtime - INTERVAL '2' SECONDS")
        .build()
    )

    //设置窗口
    val result: Table = table.window(Tumble over 5.seconds on $"rowtime" as "w")
      .groupBy($"w", $"sid")
      .select(
        $"sid",
        $"w".start() as "window_start",
        $"w".end() as "window_end",
        $"duration".sum() as "total_duration"
      )

    result.execute().print()
  }
}
