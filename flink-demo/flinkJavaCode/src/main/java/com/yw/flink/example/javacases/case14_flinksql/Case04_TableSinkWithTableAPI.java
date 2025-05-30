package com.yw.flink.example.javacases.case14_flinksql;

import org.apache.flink.table.api.*;

/**
 * Flink Table API 将数据结果写出到文件系统
 */
public class Case04_TableSinkWithTableAPI {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //设置checkpoint
        tableEnv.getConfig().getConfiguration().setLong("execution.checkpointing.interval", 5000L);

        //2.创建Source 表 - 连接Kafka
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

        Table result = tableEnv.from("station_log_tbl");

        //3.创建 Filesystem Connector 表
        tableEnv.createTemporaryTable("CsvSinkTable",
                TableDescriptor.forConnector("filesystem")
                        .schema(Schema.newBuilder()
                                .column("sid", DataTypes.STRING())
                                .column("call_out", DataTypes.STRING())
                                .column("call_in", DataTypes.STRING())
                                .column("call_type", DataTypes.STRING())
                                .column("call_time", DataTypes.BIGINT())
                                .column("duration", DataTypes.BIGINT())
                                .build())
                        .option("path", ".data/flink/output")
                        .option("sink.rolling-policy.check-interval", "2s")
                        .option("sink.rolling-policy.rollover-interval", "10s")
                        .format(FormatDescriptor.forFormat("csv")
                                .option("field-delimiter", "|")
                                .build())
                        .build()

        );

        //Table API 写出数据
//        result.insertInto("CsvSinkTable").execute();
        result.executeInsert("CsvSinkTable");
    }
}
