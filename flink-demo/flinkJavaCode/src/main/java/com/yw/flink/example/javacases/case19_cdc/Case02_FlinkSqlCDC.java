package com.yw.flink.example.javacases.case19_cdc;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink CDC 监控 Mysql 数据到Flink 处理
 */
public class Case02_FlinkSqlCDC {
    public static void main(String[] args) {
        //创建环境
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());

        //设置checkpoint
        tableEnv.getConfig().getConfiguration().setLong("execution.checkpointing.interval", 5000L);

        //ddl 方式定义 mysql cdc 源
        tableEnv.executeSql("" +
                "create table mysql_binlog(" +
                " id int ," +
                " name string," +
                " age int," +
                " primary key (id) not enforced " +
                ") with(" +
                " 'connector' = 'mysql-cdc'," +
                " 'hostname' = 'mysql_server'," +
                " 'port' = '3306'," +
                " 'username' = 'root'," +
                " 'password' = '123456'," +
                " 'database-name'='db1'," +
                " 'table-name' = 'tbl1'" +
                ")");

        //读取表 mysql_binlog 中的数据
        tableEnv.executeSql("select * from mysql_binlog").print();
    }
}
