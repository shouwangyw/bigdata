package com.yw.flink.example.scalacases.case15_flinksql

import org.apache.flink.table.api._

/**
  * Flink SQL - FullOuterJoin
  * 案例：Flink SQL 读取Kafka中订单数据和商品数据，进行fullOuterJoin
  */
object Case05_FullOuterJoin {
  def main(args: Array[String]): Unit = {
    val tableEnv: TableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build())
    //设置watermark自动推进
    tableEnv.getConfig().set("table.exec.source.idle-timeout", "5000")

    //读取Kafka - order-topic  ： order_1,product_1,20,1000
    tableEnv.executeSql("create table order_tbl(" +
      "   order_id string," +
      "   product_id string," +
      "   order_amount double," +
      "   order_time bigint," +
      "   rowtime as TO_TIMESTAMP_LTZ(order_time,3)," +
      "   watermark for rowtime as rowtime - interval '2' seconds" +
      ") with (" +
      "   'connector'='kafka'," +
      "   'topic'='order-topic'," +
      "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
      "   'properties.group.id'='testgroup'," +
      "   'scan.startup.mode'='latest-offset'," +
      "   'format'='csv'" +
      ")")
    //读取Kafka - product-topic  ： product_1,苹果,1000
    tableEnv.executeSql("create table product_tbl(" +
      "   product_id string," +
      "   product_name string," +
      "   dt bigint," +
      "   rowtime as TO_TIMESTAMP_LTZ(dt,3)," +
      "   watermark for rowtime as rowtime - interval '2' seconds" +
      ") with (" +
      "   'connector'='kafka'," +
      "   'topic'='product-topic'," +
      "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
      "   'properties.group.id'='testgroup'," +
      "   'scan.startup.mode'='latest-offset'," +
      "   'format'='csv'" +
      ")")

    //sql fulloutjoin
    val result = tableEnv.sqlQuery("select " +
      "  a.order_id,b.product_name,a.order_amount,a.order_time " +
      "from order_tbl a " +
      "full outer join product_tbl b " +
      "on a.product_id = b.product_id")

    result.execute().print()

  }

}
