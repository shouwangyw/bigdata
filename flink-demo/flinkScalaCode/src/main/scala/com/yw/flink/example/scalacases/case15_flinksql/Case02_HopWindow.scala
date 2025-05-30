package com.yw.flink.example.scalacases.case15_flinksql

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
  * Flink SQL - 滑动窗口 - HOP
  * 案例：读取Kafka中基站日志数据，每隔5秒生产10秒的窗口，统计每个基站的通话时长
  */
object Case02_HopWindow {
  def main(args: Array[String]): Unit = {
    val tableEnv: TableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build())
    //设置watermark自动推进
    tableEnv.getConfig().set("table.exec.source.idle-timeout", "5000")

    //读取Kafka 数据，通过SQL DDL定义结构
    tableEnv.executeSql("" +
      "create table station_log_tbl(" +
      "   sid string," +
      "   call_out string," +
      "   call_in string," +
      "   call_type string," +
      "   call_time bigint," +
      "   duration bigint," +
      "   rowtime AS TO_TIMESTAMP_LTZ(call_time,3)," +
      "   WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECONDS " +
      ") with (" +
      "   'connector'='kafka'," +
      "   'topic'='stationlog-topic'," +
      "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
      "   'properties.group.id'= 'testgroup'," +
      "   'scan.startup.mode'='latest-offset'," +
      "   'format'='csv'" +
      ")")

    //通过TVF 设置滚动窗口
    val result = tableEnv.sqlQuery("" +
      "select " +
      "   sid," +
      "   window_start," +
      "   window_end," +
      "   sum(duration) as total_dur " +
      "from " +
      "   TABLE(" +
      "       HOP(TABLE station_log_tbl, DESCRIPTOR(rowtime), INTERVAL '5' SECOND,interval '10' seconds )" +
      "   ) " +
      "group by sid,window_start,window_end")

    result.execute().print()
  }

}
