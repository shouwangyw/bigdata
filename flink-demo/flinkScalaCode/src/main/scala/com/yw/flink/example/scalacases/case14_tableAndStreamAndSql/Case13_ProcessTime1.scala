package com.yw.flink.example.scalacases.case14_tableAndStreamAndSql

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/** *
  * Flink Table Api和SQL编程指定 ProcessTime
  *
  */
object Case13_ProcessTime1 {
  def main(args: Array[String]): Unit = {
    val tableEnv: TableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build())

    //读取Kafka中数据，DDL方式
    tableEnv.executeSql("" +
      "create table station_log_tbl(" +
      "   sid string," +
      "   call_out string," +
      "   call_in string," +
      "   call_type string ," +
      "   duration bigint," +
      "   call_time AS PROCTIME()" +
      ") with (" +
      "   'connector' = 'kafka'," +
      "   'topic'='stationlog-topic'," +
      "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
      "   'properties.group.id'='testgroup'," +
      "   'scan.startup.mode'='latest-offset'," +
      "   'format'='csv'" +
      ")")

    //通过SQL来设置每隔5秒统计数据
    val result = tableEnv.sqlQuery("" +
      "select " +
      "TUMBLE_START(call_time,INTERVAL '5' SECOND) AS window_start," +
      "TUMBLE_END(call_time,INTERVAL '5' SECOND) AS window_end," +
      "count(sid) as cnt " +
      "from station_log_tbl " +
      "group by TUMBLE(call_time,INTERVAL '5' SECOND)")

    result.execute().print();
  }

}
