package com.yw.flink.example.javacases.case17_connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink Table Connector - HBase Connector
 * 案例：读取Hbase中数据，将结果写出到HBase
 */
public class Case05_HBaseConnector1 {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());
        tableEnv.executeSql("create table HBaseSourceTable(" +
                "   rk string," +
                "   cf1 ROW<id string,name string,age string>," +
                "   cf2 ROW<score string>," +
                "   PRIMARY KEY(rk) NOT ENFORCED" +
                ") with (" +
                "   'connector'='hbase-2.2'," +
                "   'table-name'='t1'," +
                "   'zookeeper.quorum'='node3:2181,node4:2181,node5:2181'" +
                ")");

        tableEnv.executeSql("create table HBaseSinkTable(" +
                "   rk string," +
                "   family1 ROW<id int,name string,age int>," +
                "   family2 ROW<score double>," +
                "   PRIMARY KEY(rk) NOT ENFORCED" +
                ") with (" +
                "   'connector'='hbase-2.2'," +
                "   'table-name'='t2'," +
                "   'zookeeper.quorum'='node3:2181,node4:2181,node5:2181'" +
                ")");

        //查询数据
        Table result = tableEnv.sqlQuery("select " +
                "   rk," +
                "   cast(cf1.id as int) as id," +
                "   cf1.name as name," +
                "   cast(cf1.age as int) as age," +
                "   cast(cf2.score as double) as score" +
                " from HBaseSourceTable");

        //写出数据到Hbase t2表
        tableEnv.executeSql("insert into HBaseSinkTable " +
                "select rk,ROW(id,name,age) as f1,ROW(score) as f2 from "+result);

    }
}
