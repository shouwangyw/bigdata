package com.yw.flink.example.javacases.case17_connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink Table Connector - HBase Connector
 * 案例：读取Kafka 变更日志数据，将结果写出到HBase
 */
public class Case05_HBaseConnector2 {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());

        //读取Kafka t1,t2 topic数据， ddl方式
        tableEnv.executeSql("create table KafkaSourceTable (" +
                "   id int," +
                "   name string," +
                "   age int," +
                "   score double," +
                "   primary key(id) not enforced" +
                ") with (" +
                "   'connector'='kafka'," +
                "   'topic'='t1'," +
                "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id'='testgroup'," +
                "   'scan.startup.mode'='latest-offset'," +
                "   'value.format'='debezium-json'" +
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

        //查询数据写出到Hbase t2
        tableEnv.executeSql("insert into HBaseSinkTable select " +
                " cast(id as string ) as rk," +
                " ROW(id,name,age) as family1," +
                " ROW(score) as family2 " +
                "from KafkaSourceTable");
    }
}
