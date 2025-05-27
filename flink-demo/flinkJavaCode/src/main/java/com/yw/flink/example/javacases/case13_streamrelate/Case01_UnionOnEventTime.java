package com.yw.flink.example.javacases.case13_streamrelate;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Flink 基于事件时间下的union流关联
 * 案例：读取socket中数据流形成两个流，进行Union关联后设置窗口，每隔5秒统计每个基站通话次数。
 */
public class Case01_UnionOnEventTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度为1
        env.setParallelism(1);

        //准备A流 ,002,182,183,fail,2000,20
        SingleOutputStreamOperator<StationLog> dsA = env.socketTextStream("node5", 8888)
                .map((MapFunction<String, StationLog>) line -> {
                    String[] arr = line.split(",");
                    return new StationLog(arr[0], arr[1], arr[2], arr[3], Long.valueOf(arr[4]), Long.valueOf(arr[5]));
                });
        //对A流设置watermark
        SingleOutputStreamOperator<StationLog> dsAWithWatermark = dsA
                .assignTimestampsAndWatermarks(WatermarkStrategy.<StationLog>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((SerializableTimestampAssigner<StationLog>) (stationLog, l) -> stationLog.callTime));

        //准备B流 ,002,182,183,fail,2000,20
        SingleOutputStreamOperator<StationLog> dsB = env.socketTextStream("node5", 9999).map((MapFunction<String, StationLog>) line -> {
            String[] arr = line.split(",");
            return new StationLog(arr[0], arr[1], arr[2], arr[3], Long.valueOf(arr[4]), Long.valueOf(arr[5]));
        });
        //对A流设置watermark
        SingleOutputStreamOperator<StationLog> dsBWithWatermark = dsB
                .assignTimestampsAndWatermarks(WatermarkStrategy.<StationLog>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((SerializableTimestampAssigner<StationLog>) (stationLog, l) -> stationLog.callTime));

        //两流进行union
        dsAWithWatermark.union(dsBWithWatermark).keyBy(stationLog -> stationLog.sid)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<StationLog, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<StationLog, String, String, TimeWindow>.Context context, Iterable<StationLog> elements, Collector<String> collector) throws Exception {
                        System.out.println("window - watermark 值：" + context.currentWatermark());
                        //获取窗口起始时间
                        long startTime = context.window().getStart();
                        long endTime = context.window().getEnd();

                        //统计窗口通话次数
                        int count = 0;
                        for (StationLog element : elements) {
                            count++;
                        }
                        collector.collect("窗口范围：[" + startTime + "~" + endTime + "),基站ID:" + key + ",通话次数：" + count);
                    }
                }).print();
        env.execute();
    }
}
