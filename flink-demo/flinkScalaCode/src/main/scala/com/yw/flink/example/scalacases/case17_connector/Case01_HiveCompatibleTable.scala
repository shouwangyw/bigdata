package com.yw.flink.example.scalacases.case17_connector

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.catalog.hive.HiveCatalog

/**
  * Flink SQL - 兼容表 - 操作Hive表数据，Hive表已经存在
  */
object Case01_HiveCompatibleTable {
  def main(args: Array[String]): Unit = {
    val tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());

    //创建hivecatalog
    val name = "myhive";
    val defaultDatabase = "default";
    val hiveConfDir = ".hiveconf";
    val hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
    tableEnv.registerCatalog("myhive", hive);

    //使用hive catalog
    tableEnv.useCatalog("myhive");

    //查询hive表
    tableEnv.executeSql("show tables").print();

    //向Hive中插入数据
    //    tableEnv.executeSql("insert into hive_tbl values (4,'ml',21),(5,'tq',22),(6,'gb',23)");

    //读取Hive 表中数据
    tableEnv.executeSql("select * from hive_tbl").print();
  }

}
