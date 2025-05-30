package com.yw.flink.example.scalacases.case14_tableAndStreamAndSql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.time.ZoneId

/**
  * Flink Table API 及SQL 编程中的时区和时间测试
  */
object Case12_TimeZone {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //设置时区
    tableEnv.getConfig.setLocalTimeZone(ZoneId.of("UTC"))

    val table = tableEnv.sqlQuery("select TIMESTAMP '1970-01-01 00:00:01.001' AS ntz,TO_TIMESTAMP_LTZ(4001,3) as ltz")

    tableEnv.executeSql("select CAST(ntz AS TIMESTAMP(6)) AS ntz_6, CAST(ltz AS TIMESTAMP_LTZ(6)) AS ltz_6 from " + table)
      .print()
  }
}
