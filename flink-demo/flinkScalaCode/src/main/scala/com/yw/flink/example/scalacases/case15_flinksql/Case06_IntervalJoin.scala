package com.yw.flink.example.scalacases.case15_flinksql

import org.apache.flink.table.api._

/**
  * Flink SQL - Interval Join
  * 案例：通过Flink SQL读取Kafka 中登录和点击广告数据，进行Interval Join
  */
object Case06_IntervalJoin {
  def main(args: Array[String]): Unit = {
    val tableEnv: TableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build())
    //设置watermark自动推进
    tableEnv.getConfig().set("table.exec.source.idle-timeout", "5000");

    //读取Kafka - login-topic  ： user_1,6000
    tableEnv.executeSql("create table login_tbl(" +
      "   user_id string," +
      "   login_time bigint," +
      "   rowtime as TO_TIMESTAMP_LTZ(login_time,3)," +
      "   watermark for rowtime as rowtime - interval '2' seconds" +
      ") with (" +
      "   'connector'='kafka'," +
      "   'topic'='login-topic'," +
      "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
      "   'properties.group.id'='testgroup'," +
      "   'scan.startup.mode'='latest-offset'," +
      "   'format'='csv'" +
      ")")
    //读取Kafka - click-topic  ： user_1,product_1,3000
    tableEnv.executeSql("create table click_tbl(" +
      "   user_id string," +
      "   product_id string," +
      "   dt bigint," +
      "   rowtime as TO_TIMESTAMP_LTZ(dt,3)," +
      "   watermark for rowtime as rowtime - interval '2' seconds" +
      ") with (" +
      "   'connector'='kafka'," +
      "   'topic'='click-topic'," +
      "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
      "   'properties.group.id'='testgroup'," +
      "   'scan.startup.mode'='latest-offset'," +
      "   'format'='csv'" +
      ")")

    //Interval Join
    val result = tableEnv.sqlQuery("" +
      "select " +
      "   l.user_id,l.login_time,c.product_id,c.dt " +
      "from login_tbl l " +
      "join click_tbl c " +
      "on l.user_id = c.user_id " +
      "and l.rowtime between c.rowtime - interval '2' second and c.rowtime + interval '2' seconds");

    result.execute().print()
  }

}
