package com.yw.flink.example.scalacases.case14_tableAndStreamAndSql

import com.yw.flink.example.StationLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
  * Flink Table API 状态保存测试
  */
object Case11_TableApiState {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    tableEnv.getConfig.set("table.exec.state.ttl", "5000")
    val ds: DataStream[StationLog] = env.socketTextStream("node5", 9999)
      .map(line => {
        val arr: Array[String] = line.split(",")
        StationLog(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5).toLong)
      })

    tableEnv.createTemporaryView("tbl", ds)

    val result: Table = tableEnv.sqlQuery("select sid,sum(duration) as total from tbl group by sid")
    result.execute().print()
  }

}
