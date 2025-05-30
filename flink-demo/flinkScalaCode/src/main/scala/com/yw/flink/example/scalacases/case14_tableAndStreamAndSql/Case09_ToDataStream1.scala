package com.yw.flink.example.scalacases.case14_tableAndStreamAndSql

import com.yw.flink.example.StationLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
  * Flink Table 转换成DataStream - TableEnvironment.toDataStream(table)
  */
object Case09_ToDataStream1 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val ds: DataStream[StationLog] = env.socketTextStream("node5", 9999)
      .map(line => {
        val arr: Array[String] = line.split(",")
        StationLog(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5).toLong)
      })

    tableEnv.createTemporaryView("xx", ds);

    val table: Table = tableEnv.from("xx")

    val result: DataStream[Row] = tableEnv.toDataStream(table)
    result.print()
    env.execute()
  }

}
