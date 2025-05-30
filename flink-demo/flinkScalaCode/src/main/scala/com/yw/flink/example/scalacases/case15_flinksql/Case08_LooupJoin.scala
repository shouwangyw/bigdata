package com.yw.flink.example.scalacases.case15_flinksql

import org.apache.flink.table.api._

/**
  * Flink SQL - Lookup Join
  * 案例：读取Kafka中数据形成表 查询 MySQL中的维度表
  */
object Case08_LooupJoin {
  def main(args: Array[String]): Unit = {
    val tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build())

    //设置watermark自动推进
    tableEnv.getConfig().set("table.exec.source.idle-timeout", "5000")

    //读取Kafka - visit-topic  ： p_002,1000
    tableEnv.executeSql("create table visit_tbl(" +
      "   product_id string," +
      "   visit_dt bigint," +
      "   proc_time as proctime()," +
      "   rowtime as TO_TIMESTAMP_LTZ(visit_dt,3)," +
      "   watermark for rowtime as rowtime - interval '2' seconds" +
      ") with (" +
      "   'connector'='kafka'," +
      "   'topic'='visit-topic'," +
      "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
      "   'properties.group.id'='testgroup'," +
      "   'scan.startup.mode'='latest-offset'," +
      "   'format'='csv'" +
      ")")

    //读取MySQL 中维度数据 ，DDL方式创建表
    tableEnv.executeSql("" +
      "create table product_tbl(" +
      "   product_id string," +
      "   product_name string," +
      "   price double " +
      ") with (" +
      "   'connector'='jdbc'," +
      "   'url'='jdbc:mysql://node2:3306/mydb?useSSL=false'," +
      "   'table-name'='product_tbl'," +
      "   'username'='root'," +
      "   'password'='123456' " +
      ")")

    //查询维度表数据
    val result = tableEnv.sqlQuery("select " +
      "   l.product_id," +
      "   l.visit_dt," +
      "   r.product_name," +
      "   r.price " +
      "from visit_tbl l " +
      "join product_tbl for system_time as of l.proc_time r " +
      "on l.product_id = r.product_id")

    result.execute().print()
  }


}
