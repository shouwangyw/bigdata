package com.yw.flink.example.scalacases.case14_tableAndStreamAndSql

import com.yw.flink.example.StationLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{Schema, Table}

/**
  * FlinK Table API 及SQL编程 - 指定EventTime
  */
object Case14_EventTime2 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //设置自定推进watermark
    tableEnv.getConfig().set("table.exec.source.idle-timeout", "5000");

    val ds: DataStream[StationLog] = env.socketTextStream("node5", 9999)
      .map(line => {
        val arr: Array[String] = line.split(",")
        StationLog(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5).toLong)
      })

    val table: Table = tableEnv.fromDataStream(ds, Schema.newBuilder()
      .columnByExpression("time_ltz", "TO_TIMESTAMP_LTZ(callTime,3)")
      .watermark("time_ltz", "time_ltz - interval '2' seconds")
      .build())

    //设置5s一个窗口统计数据
    val result = tableEnv.sqlQuery("" +
      "select " +
      "TUMBLE_START(time_ltz,INTERVAL '5' SECOND) AS window_start," +
      "TUMBLE_END(time_ltz,INTERVAL '5' SECOND) AS window_end," +
      "count(sid) as cnt " +
      "from  " + table +
      " group by TUMBLE(time_ltz,INTERVAL '5' SECOND)")

    result.execute().print()
  }
}
