package com.yw.flink.example.javacases.case14_tableAndStreamAndSql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 使用Table API 和SQL混合查询表
 * 案例：通过读取Kafka中基站日志数据，进行分析
 */
public class Case03_QueryTableWithMix {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //1.创建环境
//        EnvironmentSettings settings = EnvironmentSettings.newInstance()
//                .inStreamingMode()
//                .build();
//        TableEnvironment tableEnv = TableEnvironment.create(settings);

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
                        .option("topic","station_log-topic")
                        .option("properties.bootstrap.servers","node1:9092,node2:9092,node3:9092")
                        .option("properties.group.id","testgroup")
                        .option("scan.startup.mode","latest-offset")
                        .option("format","csv")
                        .build()
        );

        //读取数据
        Table station_log_tbl = tableEnv.from("station_log_tbl");

        //读取数据，统计每个基站的通话总时长（通话成功数据并且通话时长大于10）
        Table filter = station_log_tbl.filter($("call_type").isEqual("success").and($("duration").isGreater(10)));

        //使用SQL继续查询数据
        Table result = tableEnv.sqlQuery("select sid,sum(duration) as total_duration from " + filter + " group by sid");
        DataStream<Row> end = tableEnv.toDataStream(result);
        end.print();
        env.execute();

//        result.execute().print();
    }
}
