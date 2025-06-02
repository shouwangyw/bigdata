package com.yw.flink.example.javacases.case19_cdc;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink CDC 同步MySQL数据到HBase
 */
public class Case07_SqlCDCMysqlToHbase {
    public static void main(String[] args) {
        //创建环境
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());

        //设置checkpoint
        tableEnv.getConfig().getConfiguration().setLong("execution.checkpointing.interval",5000L);

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

        // DDL定义Hbase sink 表
        tableEnv.executeSql("create table HBaseSinkTbl (" +
                " rk string," +
                " cf1 ROW<id string,name string,age string>," +
                " PRIMARY KEY (rk)  NOT ENFORCED" +
                ") with (" +
                " 'connector' = 'hbase-2.2'," +
                " 'table-name'='tbl_cdc'," +
                " 'zookeeper.quorum'='node3:2181,node4:2181,node5:2181'" +
                ")");

        //将MySQL数据插入到HBase中
        tableEnv.executeSql("insert into HBaseSinkTbl select " +
                "cast (id as string) as rk ," +
                "ROW(CAST (id as string),name,cast (age as string)) as cf1 " +
                " from mysql_binlog");
    }
}
