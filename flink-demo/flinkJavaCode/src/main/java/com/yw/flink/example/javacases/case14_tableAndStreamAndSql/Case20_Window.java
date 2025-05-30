package com.yw.flink.example.javacases.case14_tableAndStreamAndSql;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * Flink Table API - 窗口使用
 * 案例：读取socket基站日志数据，每隔5秒设置滚动窗口统计每个基站的通话时长
 */
public class Case20_Window {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //watermark自动推进
        tableEnv.getConfig().set("table.exec.source.idle-timeout", "5000");

        //读取socket-9999数据
        SingleOutputStreamOperator<StationLog> ds = env.socketTextStream("node5", 9999)
                .map((MapFunction<String, StationLog>) s -> {
                    String[] split = s.split(",");
                    return new StationLog(split[0], split[1], split[2], split[3], Long.valueOf(split[4]), Long.valueOf(split[5]));
                });

        //ds -> table
        Table table = tableEnv.fromDataStream(ds,
                Schema.newBuilder()
                        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(callTime,3)")
                        .watermark("rowtime", "rowtime - INTERVAL '2' SECONDS")
                        .build()

        );

        //设置每隔5秒的滚动窗口
        Table result = table.window(
                        Tumble.over(lit(5).seconds()).on($("rowtime")).as("w")
                ).groupBy($("w"), $("sid"))
                .select(
                        $("sid"),
                        $("w").start().as("window_start"),
                        $("w").end().as("window_end"),
                        $("duration").sum().as("total_duration")
                );

        result.execute().print();
    }
}

