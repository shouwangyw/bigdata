package com.yw.flink.example.javacases.case14_flinksql;

import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 使用Table API 查询表
 * 案例：通过读取Kafka中基站日志数据，进行分析
 */
public class Case01_QueryTableWithTableApi {
    public static void main(String[] args) {
        //1.创建环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //2.创建Source 表 - 连接Kafka
        //001,181,182,success,1000,40
        tableEnv.createTemporaryTable("station_log_tbl",
                TableDescriptor.forConnector("kafka")
                        .schema(Schema.newBuilder()
                                .column("sid", DataTypes.STRING())
                                .column("call_out", DataTypes.STRING())
                                .column("call_in", DataTypes.STRING())
                                .column("call_type", DataTypes.STRING())
                                .column("call_time", DataTypes.BIGINT())
                                .column("duration", DataTypes.BIGINT())
                                .build())
                        .option("topic", "stationlog-topic")
                        .option("properties.bootstrap.servers", "node1:9092,node2:9092,node3:9092")
                        .option("properties.group.id", "testgroup")
                        .option("scan.startup.mode", "latest-offset")
                        .option("format", "csv")
                        .build()
        );

        //读取数据
        Table stationLogTbl = tableEnv.from("station_log_tbl");

        //读取数据，统计每个基站的通话总时长（通话成功数据并且通话时长大于10）
        Table result = stationLogTbl.filter($("call_type").isEqual("success").and($("duration").isGreater(10)))
                .groupBy($("sid"))
                .select($("sid"), $("duration").sum().as("total_duration"));

        result.execute().print();


    }
}
