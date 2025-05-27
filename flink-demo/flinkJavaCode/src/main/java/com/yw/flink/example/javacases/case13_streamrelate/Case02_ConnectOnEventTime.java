package com.yw.flink.example.javacases.case13_streamrelate;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Flink - Connect 两流合并，测试watermark
 */
public class Case02_ConnectOnEventTime {
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


        //准备B流 ,002,2000
        SingleOutputStreamOperator<String> dsB = env.socketTextStream("node5", 9999);
        //对A流设置watermark
        SingleOutputStreamOperator<String> dsBWithWatermark = dsB
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((SerializableTimestampAssigner<String>) (str, l) -> Long.parseLong(str.split(",")[1])));

        //两流进行union
        dsAWithWatermark.connect(dsBWithWatermark)
                .process(new CoProcessFunction<StationLog, String, String>() {
                    //处理第一个流数据
                    @Override
                    public void processElement1(StationLog stationLog,
                                                CoProcessFunction<StationLog, String, String>.Context context,
                                                Collector<String> collector) throws Exception {
                        collector.collect("A流数据：" + stationLog.toString() + ",watermark = " + context.timerService().currentWatermark());
                    }

                    //处理第二个流中数据
                    @Override
                    public void processElement2(String str,
                                                CoProcessFunction<StationLog, String, String>.Context context,
                                                Collector<String> collector) throws Exception {
                        collector.collect("B流数据：" + str + "，watermark = " + context.timerService().currentWatermark());
                    }
                }).print();
        env.execute();
    }
}
