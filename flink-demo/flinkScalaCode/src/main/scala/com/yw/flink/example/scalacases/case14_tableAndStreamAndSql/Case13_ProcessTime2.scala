package com.yw.flink.example.scalacases.case14_tableAndStreamAndSql

import com.yw.flink.example.StationLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{Schema, Table}

/** *
  * Flink Table Api和SQL编程指定 ProcessTime
  */
object Case13_ProcessTime2 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val ds: DataStream[StationLog] = env.socketTextStream("node5", 9999)
      .map(line => {
        val arr: Array[String] = line.split(",")
        StationLog(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5).toLong)
      })

    val table: Table = tableEnv.fromDataStream(ds, Schema.newBuilder()
      .columnByExpression("proc_time", "PROCTIME()")
      .build())

    val result = tableEnv.sqlQuery("" +
      "select " +
      "TUMBLE_START(proc_time,INTERVAL '5' SECOND) AS window_start," +
      "TUMBLE_END(proc_time,INTERVAL '5' SECOND) AS window_end," +
      "count(sid) as cnt " +
      "from " + table +
      " group by TUMBLE(proc_time,INTERVAL '5' SECOND)")

    result.execute().print()
  }

}
