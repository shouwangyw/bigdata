package com.yw.flink.example.javacases.case14_tableAndStreamAndSql;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink DataStream转换成Table - tableEnvironment.fromDataStream(dataStream,Schema)
 */
public class Case06_FromDataStream2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建TableEnvironment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<StationLog> stationLogDs = env.socketTextStream("node5", 9999).map((MapFunction<String, StationLog>) line -> {
            String[] arr = line.split(",");
            return new StationLog(arr[0], arr[1], arr[2], arr[3], Long.valueOf(arr[4]), Long.valueOf(arr[5]));
        });

        //通过Schema指定列的类型，新列、指定时间列、指定watermark
        Table table = tableEnv.fromDataStream(stationLogDs, Schema.newBuilder()
//                        .column("sid", DataTypes.STRING())
//                        .column("callOut", DataTypes.STRING())
                .columnByExpression("proc_time", "PROCTIME()")
                .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(callTime,3)")
                .watermark("rowtime", "rowtime - INTERVAL '2' SECOND").build());

        table.printSchema();
        table.execute().print();
    }
}
