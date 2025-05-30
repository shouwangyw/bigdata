package com.yw.flink.example.javacases.case14_tableAndStreamAndSql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import java.time.ZoneId;

/**
 * Flink Table API 及SQL 编程中的时区和时间测试
 */
public class Case12_TimeZone {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(
                EnvironmentSettings.newInstance().inStreamingMode().build());

        //设置时区
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
//        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));

        //测试时间类型
        Table table = tableEnv.sqlQuery("select " +
                "TIMESTAMP '1970-01-01 00:00:01.001' AS ntz," +
                "TO_TIMESTAMP_LTZ(4001,3) as ltz");

//        table.printSchema();
//        table.execute().print();

        tableEnv.executeSql("" +
                "select " +
                " CAST(ntz AS TIMESTAMP(6)) AS ntz_6," +
                " CAST(ltz AS TIMESTAMP_LTZ(6)) AS ltz_6" +
                " from " + table).print();
    }
}
