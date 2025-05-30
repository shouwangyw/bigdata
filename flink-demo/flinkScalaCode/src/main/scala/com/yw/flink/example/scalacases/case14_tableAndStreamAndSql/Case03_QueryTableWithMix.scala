package com.yw.flink.example.scalacases.case14_tableAndStreamAndSql

import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment, TableResult}

/**
  * 使用Table API 和SQL混合查询表
  * 案例：通过读取Kafka中基站日志数据，进行分析
  */
object Case03_QueryTableWithMix {
  def main(args: Array[String]): Unit = {
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .build()
    val tableEnv: TableEnvironment = TableEnvironment.create(settings)

    //通过SQL DDL 创建Source 表
    tableEnv.executeSql("create table  station_log_tbl (" +
      "   sid string," +
      "   call_out string," +
      "   call_in string," +
      "   call_type string," +
      "   call_time bigint," +
      "   duration bigint" +
      ") with (" +
      "   'connector' = 'kafka'," +
      "   'topic'='stationlog-topic'," +
      "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
      "   'properties.group.id'='testgroup'," +
      "   'scan.startup.mode'='latest-offset'," +
      "   'format'='csv'" +
      ")")

    val table: Table = tableEnv.from("station_log_tbl")
    val filter: Table = table.filter($"call_type".isEqual("success").and($"duration".isGreater(10)))

    //SQL查询数据
    val result: TableResult = tableEnv.executeSql("select sid,sum(duration) as total_duration from " + filter + " group by sid")
    result.print()
  }

}
