package com.yw.flink.example.javacases.case14_tableAndStreamAndSql;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Flink Table API orderBy
 */
public class Case16_OrderBy {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建TableEnvironment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<StationLog> stationLogDs = env.socketTextStream("node5", 9999)
                .map((MapFunction<String, StationLog>) line -> {
                    String[] arr = line.split(",");
                    return new StationLog(arr[0], arr[1], arr[2], arr[3], Long.valueOf(arr[4]), Long.valueOf(arr[5]));
                });

        Table table = tableEnv.fromDataStream(stationLogDs,
                Schema.newBuilder()
                        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(callTime,3)")
                        .watermark("rowtime", "rowtime - INTERVAL '2' SECOND")
                        .build()
        );

        Table result = table.orderBy($("duration").asc()).offset(2).fetch(3);
        result.execute().print();
    }
}
