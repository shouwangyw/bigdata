package com.yw.flink.example.javacases.case14_tableAndStreamAndSql;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.OverWindowedTable;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.*;

/**
 * Flink Table API - Over 开窗函数
 * 案例：读取基站日志数据，Over开窗函数统计每条数据最近2条该基站通话总时长
 */
public class Case17_OverWindowBaseRowCount {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //自动推进watermark
        tableEnv.getConfig().set("table.exec.source.idle-timeout","5000");

        SingleOutputStreamOperator<StationLog> ds = env.socketTextStream("node5", 9999)
                .map((MapFunction<String, StationLog>) s -> {
                    String[] split = s.split(",");
                    return new StationLog(split[0], split[1], split[2], split[3], Long.valueOf(split[4]), Long.valueOf(split[5]));
                });

        //将DataStream转换成Table
        Table table = tableEnv.fromDataStream(ds, Schema.newBuilder()
                .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(callTime,3)")
                .watermark("rowtime", "rowtime - INTERVAL '2' SECONDS")
                .build());

        //设置Over开窗函数
        OverWindowedTable window = table.window(Over
                .partitionBy($("sid"))
                .orderBy($("rowtime"))
                .preceding(rowInterval(2L))
                .following(CURRENT_ROW)
                .as("w")
        );

        //统计
        Table result = window.select(
                $("sid"),
                $("duration"),
                $("callTime"),
                $("duration").sum().over($("w")).as("sum_duration")
        );

        result.execute().print();
    }
}
