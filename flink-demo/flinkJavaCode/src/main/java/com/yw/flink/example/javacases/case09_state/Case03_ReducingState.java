package com.yw.flink.example.javacases.case09_state;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Flink ReducingState 状态测试
 * 案例：读取基站通话数据，每隔20s统计每个主叫号码通话总时长。
 * 001,186,187,busy,1000,10
 * 002,187,186,fail,2000,20
 * 003,186,188,busy,3000,30
 * 004,187,186,busy,4000,40
 * 005,189,187,busy,5000,50
 */
public class Case03_ReducingState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取socket数据
        DataStreamSource<String> ds = env.socketTextStream("nc_server", 9999);
        //转换数据 ds -> StationLog
        SingleOutputStreamOperator<StationLog> stationLogDS = ds.map((MapFunction<String, StationLog>) line -> {
            String[] split = line.split(",");
            return new StationLog(
                    split[0],
                    split[1],
                    split[2],
                    split[3],
                    Long.valueOf(split[4]),
                    Long.valueOf(split[5]));
        });

        //对 stationLogDS 进行keyby ,key选择的是主叫
        KeyedStream<StationLog, String> keyedStream = stationLogDS.keyBy((KeySelector<StationLog, String>) stationLog -> stationLog.callOut);

        keyedStream.process(new KeyedProcessFunction<String, StationLog, String>() {

            private ReducingState<Long> reducingState;

            //注册状态并获取状态
            @Override
            public void open(Configuration parameters) throws Exception {
                //创建reducingState 状态描述器注册状态
                ReducingStateDescriptor<Long> reducingStateDescriptor = new ReducingStateDescriptor<>("callDuration-state",
                        new ReduceFunction<Long>() {
                            @Override
                            public Long reduce(Long v1, Long v2) throws Exception {
                                return v1 + v2;
                            }
                        }, Long.class);
                //获取状态
                reducingState = getRuntimeContext().getReducingState(reducingStateDescriptor);
            }

            //每来一条数据处理一次
            @Override
            public void processElement(StationLog stationLog,
                                       KeyedProcessFunction<String, StationLog, String>.Context context,
                                       Collector<String> collector) throws Exception {
                Long totalCallTime = reducingState.get();
                if (totalCallTime == null) {
                    //获取当前时间
                    long time = context.timerService().currentProcessingTime();
                    //注册定时器
                    context.timerService().registerProcessingTimeTimer(time + 20 * 1000L);
                }

                //将当前条数据通话时长加入到状态中
                reducingState.add(stationLog.duration);
            }

            //定时器触发方法
            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, StationLog, String>.OnTimerContext ctx,
                                Collector<String> out) throws Exception {
                Long totalCallTime = reducingState.get();
                out.collect("主叫号码：" + ctx.getCurrentKey() + ",近20s通话时长 ：" + totalCallTime);
                //清空状态
                reducingState.clear();
            }
        }).print();
        env.execute();
    }
}
