package com.yw.flink.example.scalacases.case14_flinksql

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * Flink SQL 将数据写出到外部文件系统
 */
object Case05_TableSinkWithSQL {
  def main(args: Array[String]): Unit = {
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .build()
    val tableEnv: TableEnvironment = TableEnvironment.create(settings)

    tableEnv.getConfig.getConfiguration.setLong("execution.checkpointing.interval",5000L)

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

    //通过SQL DDL 方式创建 filesystem表
    tableEnv.executeSql("create table CsvSinkTable (" +
      "   sid string," +
      "   call_out string," +
      "   call_in string," +
      "   call_type string," +
      "   call_time bigint," +
      "   duration bigint" +
      ") with (" +
      "   'connector' = 'filesystem'," +
      "   'path'='.data/flinkdata/output'," +
      "   'sink.rolling-policy.check-interval'='2s'," +
      "   'sink.rolling-policy.rollover-interval'='10s'," +
      "   'format'='csv'," +
      "   'csv.field-delimiter'='|'" +
      ")")

    //执行sql 将结果写出到文件系统
    tableEnv.executeSql("insert into CsvSinkTable select * from station_log_tbl")

  }

}
