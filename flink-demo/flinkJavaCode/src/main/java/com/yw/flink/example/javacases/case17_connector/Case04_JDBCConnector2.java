package com.yw.flink.example.javacases.case17_connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink Table Connector - Jdbc Connector
 * 案例：读取Kafka中变更日志数据，写出到MySql中
 */
public class Case04_JDBCConnector2 {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());

        //读取Kafka t1,t2 topic数据， ddl方式
        tableEnv.executeSql("create table KafkaSourceTable (" +
                "   id int," +
                "   name string," +
                "   age int," +
                "   primary key(id) not enforced" +
                ") with (" +
                "   'connector'='kafka'," +
                "   'topic'='t1'," +
                "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id'='testgroup'," +
                "   'scan.startup.mode'='latest-offset'," +
                "   'value.format'='debezium-json'" +
                ")");

        //MySQL Sink DDL
        tableEnv.executeSql("create table MysqlSinkTable(" +
                "   id int ," +
                "   name string," +
                "   age int," +
                "   PRIMARY KEY (id) NOT ENFORCED" +
                ") with (" +
                "   'connector'='jdbc'," +
                "   'url'='jdbc:mysql://node2:3306/mydb?useSSL=false'," +
                "   'table-name'='t2'," +
                "   'username'='root'," +
                "   'password'='123456'" +
                ")");

        //sql 写出数据
        tableEnv.executeSql("insert into MysqlSinkTable select id ,name ,age from KafkaSourceTable ");
    }
}
