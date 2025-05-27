package com.yw.flink.example.javacases.case12_windowapi;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 使用侧流将迟到严重的数据进行收集
 */
public class Case12_SideOutputLateData {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //将数据转换成StationLog对象
        SingleOutputStreamOperator<StationLog> stationLogDS = env.socketTextStream("node5", 9999)
                .map((MapFunction<String, StationLog>) s -> {
                    String[] arr = s.split(",");
                    return new StationLog(arr[0].trim(), arr[1].trim(), arr[2].trim(), arr[3].trim(), Long.valueOf(arr[4]), Long.valueOf(arr[5]));
                });

        //设置水位线
        SingleOutputStreamOperator<StationLog> dsWithWatermark = stationLogDS.assignTimestampsAndWatermarks(
                //设置watermark ,延迟时间为2s
                WatermarkStrategy.<StationLog>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        //设置时间戳列信息
                        .withTimestampAssigner((stationLog, timestamp) -> stationLog.callTime)
                        //设置并行度空闲时间，方便推进水位线
                        .withIdleness(Duration.ofSeconds(5)));

        //按照基站ID进行分组，并每隔5s统计每个基站所有主叫通话总时长
        KeyedStream<StationLog, String> keyedStream = dsWithWatermark
                .keyBy((KeySelector<StationLog, String>) stationLog -> stationLog.sid);

        //创建outputag
        OutputTag<StationLog> lateOutputTag = new OutputTag<StationLog>("late-data") {
        };

        //每隔5s统计每个基站所有主叫通话总时长，使用事件时间
        SingleOutputStreamOperator<String> result = keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //在watermark 允许数据延迟的基础上，再等一等延迟数据，等2s
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(lateOutputTag)
                .process(new ProcessWindowFunction<StationLog, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<StationLog, String, String, TimeWindow>.Context context, Iterable<StationLog> elements, Collector<String> out) throws Exception {
                        //统计每个基站所有主叫通话总时长
                        long sumCallTime = 0L;
                        for (StationLog element : elements) {
                            sumCallTime += element.duration;
                        }

                        //获取窗口起始时间
                        long start = context.window().getStart() < 0 ? 0 : context.window().getStart();
                        long end = context.window().getEnd();

                        out.collect("窗口范围：[" + start + "~" + end + "),基站：" + key + ",所有主叫通话总时长：" + sumCallTime);
                    }
                });

        result.print("窗口正常数据>>>>");
        //获取迟到数据
        result.getSideOutput(lateOutputTag).print("迟到数据>>>>");
        env.execute();
    }
}
