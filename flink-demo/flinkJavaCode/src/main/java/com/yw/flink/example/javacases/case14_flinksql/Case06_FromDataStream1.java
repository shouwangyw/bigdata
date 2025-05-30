package com.yw.flink.example.javacases.case14_flinksql;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink DataStream转换成Table - tableEnvironment.fromDataStream(...)
 */
public class Case06_FromDataStream1 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建TableEnvironment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<StationLog> stationLogDs = env.socketTextStream("node5", 9999)
                .map((MapFunction<String, StationLog>) line -> {
                    String[] arr = line.split(",");
                    return new StationLog(arr[0], arr[1], arr[2], arr[3], Long.valueOf(arr[4]), Long.valueOf(arr[5]));
                });

        //将DataStream转换成Table对象
        Table table = tableEnv.fromDataStream(stationLogDs);

        table.printSchema();
        table.execute().print();
    }
}
