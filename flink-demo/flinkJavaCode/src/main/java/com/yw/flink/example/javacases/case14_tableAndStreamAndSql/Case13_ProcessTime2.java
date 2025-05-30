package com.yw.flink.example.javacases.case14_tableAndStreamAndSql;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/***
 * Flink Table Api和SQL编程指定 ProcessTime
 */
public class Case13_ProcessTime2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建TableEnvironment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<StationLog> ds = env.socketTextStream("node5", 9999)
                .map((MapFunction<String, StationLog>) line -> {
                    String[] arr = line.split(",");
                    return new StationLog(arr[0], arr[1], arr[2], arr[3], Long.valueOf(arr[4]), Long.valueOf(arr[5]));
                });

        //将DataStream转换成Table
        Table table = tableEnv.fromDataStream(ds, Schema.newBuilder()
                .columnByExpression("proc_time", "PROCTIME()")
                .build());

        //设置窗口
        Table result = tableEnv.sqlQuery("" +
                "select " +
                "TUMBLE_START(proc_time,INTERVAL '5' SECOND) AS window_start," +
                "TUMBLE_END(proc_time,INTERVAL '5' SECOND) AS window_end," +
                "count(sid) as cnt " +
                "from " + table +
                " group by TUMBLE(proc_time,INTERVAL '5' SECOND)");

        result.execute().print();
    }
}
