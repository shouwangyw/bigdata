package com.yw.flink.example.javacases.case07_processfun;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Flink读取Socket中通话数据，如果被叫手机连续5s呼叫失败生成告警信息。
 * <p>
 * 002,187,186,fail,2000,20
 * 002,187,188,success,2000,20
 */
public class ProcessFun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //002,187,186,fail,2000,20
        DataStreamSource<String> ds = env.socketTextStream("node5", 9999);

        //对数据进行转换
        SingleOutputStreamOperator<StationLog> ds2 = ds.map(new MapFunction<String, StationLog>() {
            @Override
            public StationLog map(String s) throws Exception {
                String[] split = s.split(",");
                return new StationLog(split[0], split[1], split[2], split[3], Long.valueOf(split[4]), Long.valueOf(split[5]));
            }
        });

        //对被叫手机进行分组
        KeyedStream<StationLog, String> keyedStream = ds2.keyBy(new KeySelector<StationLog, String>() {
            @Override
            public String getKey(StationLog stationLog) throws Exception {
                return stationLog.callIn;
            }
        });

        //使用processfunction 进行处理，针对每个被叫手机注册定时器
        SingleOutputStreamOperator<String> result = keyedStream.process(new KeyedProcessFunction<String, StationLog, String>() {
            //定义Flink 状态 - 保存触发器触发的时间
            ValueState<Long> timeState = null;

            //在open 方法中对时间状态初始化
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("time", Long.class);
                timeState = getRuntimeContext().getState(descriptor);
            }

            //每条数据进来后都会调用
            @Override
            public void processElement(StationLog stationLog, KeyedProcessFunction<String, StationLog, String>.Context ctx, Collector<String> collector) throws Exception {
                //002,187,186,fail,2000,20
                //002,187,188,success,2000,20
                //获取该key(被叫手机)定时器触发的时间状态
                Long time = timeState.value();

                if ("fail".equals(stationLog.callType) && time == null) {
                    //数据通话失败，需要注册定时器
                    //获取当前时间
                    long nowTime = ctx.timerService().currentProcessingTime();
                    //设置定时器触发时间，5秒后触发
                    long triggerTime = nowTime + 5000L;
                    //注册定时器
                    ctx.timerService().registerProcessingTimeTimer(triggerTime);

                    //对状态中保存的触发器时间进行更新
                    timeState.update(triggerTime);
                }

                if (!"fail".equals(stationLog.callType) && time != null) {
                    //在定时器触发之前，如果该被叫手机号 呼叫非失败，要取消定时器
                    ctx.timerService().deleteProcessingTimeTimer(time);
                    //清除状态值
                    timeState.clear();
                }


            }

            //定时器触发方法
            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, StationLog, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                out.collect("触发时间 = " + timestamp + ",被叫手机号：" + ctx.getCurrentKey() + "连续5秒呼叫失败！！！");

                //清空定时器
                timeState.clear();

            }
        });

        result.print();

        env.execute();
    }
}
