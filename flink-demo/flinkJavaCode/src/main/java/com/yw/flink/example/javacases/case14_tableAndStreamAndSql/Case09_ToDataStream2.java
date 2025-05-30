package com.yw.flink.example.javacases.case14_tableAndStreamAndSql;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink Table 转换成DataStream - TableEnvironment.toDataStream(table,abstractDataType)
 */
public class Case09_ToDataStream2 {
    public static void main(String[] args) throws Exception {
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

        DataStream<StationLog> result = tableEnv.toDataStream(table, DataTypes.STRUCTURED(
                StationLog.class,
                DataTypes.FIELD("sid", DataTypes.STRING()),
                DataTypes.FIELD("callOut", DataTypes.STRING()),
                DataTypes.FIELD("callIn", DataTypes.STRING()),
                DataTypes.FIELD("callType", DataTypes.STRING()),
                DataTypes.FIELD("callTime", DataTypes.BIGINT()),
                DataTypes.FIELD("duration", DataTypes.BIGINT())
        ));

        result.print();
        env.execute();
    }
}
