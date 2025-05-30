package com.yw.flink.example.javacases.case14_tableAndStreamAndSql;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Flink Table 转换成DataStream - tableEnv.toChangelogStream(table)
 * 1.创建DataStream
 * 2.设置watermark
 * 3.dataStream -> Table (传递watermark)
 * 4.toChangelogStream:table -> DataStream ： 观察是否有watermark传递
 */
public class Case10_ToChangelogStream1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //创建DataStream
        SingleOutputStreamOperator<StationLog> stationLogDs = env.socketTextStream("node5", 9999)
                .map((MapFunction<String, StationLog>) line -> {
                    String[] arr = line.split(",");
                    return new StationLog(arr[0], arr[1], arr[2], arr[3], Long.valueOf(arr[4]), Long.valueOf(arr[5]));
                });

        //设置watermark
        SingleOutputStreamOperator<StationLog> dsWithWatermark = stationLogDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<StationLog>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((SerializableTimestampAssigner<StationLog>) (stationLog, l) -> stationLog.callTime)
        );

        //dataStream -> Table (传递watermark)
        Table table = tableEnv.fromDataStream(dsWithWatermark, Schema.newBuilder()
                .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(callTime,3)")
                .watermark("rowtime", "SOURCE_WATERMARK()")
                .build());

        //对table进行聚合
        Table resultTbl = table.groupBy($("sid"))
                .select($("sid"), $("duration").sum().as("total_duration"));

        //table -> DataStream
        DataStream<Row> result = tableEnv.toChangelogStream(resultTbl);
        result.process(new ProcessFunction<Row, String>() {
            @Override
            public void processElement(Row row,
                                       ProcessFunction<Row, String>.Context context,
                                       Collector<String> collector) throws Exception {
                collector.collect("数据：" + row + ",watermark:" + context.timerService().currentWatermark());
            }
        }).print();

        env.execute();
    }
}
