package com.yw.flink.example.javacases.case17_connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink Table Connector- Jdbc Connector
 * 案例：读取mysql中数据写出到mysql
 */
public class Case04_JDBCConnector1 {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());
        tableEnv.executeSql("create table MysqlSourceTable(" +
                "   id int ," +
                "   name string," +
                "   age int" +
                ") with (" +
                "   'connector'='jdbc'," +
                "   'url'='jdbc:mysql://node2:3306/mydb?useSSL=false'," +
                "   'table-name'='t1'," +
                "   'username'='root'," +
                "   'password'='123456'" +
                ")");
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
        tableEnv.executeSql("insert into MysqlSinkTable select id ,name ,age from MysqlSourceTable ");


    }
}
