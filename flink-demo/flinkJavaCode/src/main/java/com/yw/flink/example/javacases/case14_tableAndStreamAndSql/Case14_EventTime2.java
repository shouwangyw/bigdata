package com.yw.flink.example.javacases.case14_tableAndStreamAndSql;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * FlinK Table API 及SQL编程 - 指定EventTime
 */
public class Case14_EventTime2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建TableEnvironment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //设置自定推进watermark
        tableEnv.getConfig().set("table.exec.source.idle-timeout", "5000");

        SingleOutputStreamOperator<StationLog> ds = env.socketTextStream("node5", 9999)
                .map((MapFunction<String, StationLog>) line -> {
                    String[] arr = line.split(",");
                    return new StationLog(arr[0], arr[1], arr[2], arr[3], Long.valueOf(arr[4]), Long.valueOf(arr[5]));
                });

        //dataStream转换成Table
        Table table = tableEnv.fromDataStream(ds, Schema.newBuilder()
                .columnByExpression("time_ltz", "TO_TIMESTAMP_LTZ(callTime,3)")
                .watermark("time_ltz", "time_ltz - INTERVAL '2' second")
                .build());

        //设置5s一个窗口统计数据
        Table result = tableEnv.sqlQuery("" +
                "select " +
                "TUMBLE_START(time_ltz,INTERVAL '5' SECOND) AS window_start," +
                "TUMBLE_END(time_ltz,INTERVAL '5' SECOND) AS window_end," +
                "count(sid) as cnt " +
                "from  " + table +
                " group by TUMBLE(time_ltz,INTERVAL '5' SECOND)");

        result.execute().print();

    }
}
