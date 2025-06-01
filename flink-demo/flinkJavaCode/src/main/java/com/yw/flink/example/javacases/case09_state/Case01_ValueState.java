package com.yw.flink.example.javacases.case09_state;

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
 * Flink ValueState 状态测试
 * 案例：读取基站日志数据，统计每个主叫手机通话间隔时间，单位为毫秒
 * 001,186,187,busy,1000,10
 * 002,187,186,fail,2000,20
 * 003,186,188,busy,3000,30
 * 004,187,186,busy,4000,40
 * 005,189,187,busy,5000,50
 */
public class Case01_ValueState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取socket数据
        DataStreamSource<String> ds = env.socketTextStream("nc_server", 9999);
        //转换数据 ds -> StationLog
        SingleOutputStreamOperator<StationLog> stationLogDS = ds.map(new MapFunction<String, StationLog>() {
            @Override
            public StationLog map(String line) throws Exception {
                String[] split = line.split(",");
                return new StationLog(split[0], split[1], split[2], split[3], Long.valueOf(split[4]), Long.valueOf(split[5]));
            }
        });

        //对 stationLogDS 进行keyby ,key选择的是主叫
        KeyedStream<StationLog, String> keyedStream = stationLogDS.keyBy((KeySelector<StationLog, String>) stationLog -> stationLog.callOut);

        //状态编程，统计每个主叫号码两次通话间隔时长
        SingleOutputStreamOperator<String> result = keyedStream.process(new KeyedProcessFunction<String, StationLog, String>() {

            private ValueState<Long> callTimeValueState;

            //状态注册与获取
            @Override
            public void open(Configuration parameters) throws Exception {
                //1.定义状态描述器
                ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("callTimeValueState", Long.class);
                //2.获取状态
                callTimeValueState = getRuntimeContext().getState(descriptor);
            }

            //来一条数据处理一条数据
            @Override
            public void processElement(StationLog stationLog, KeyedProcessFunction<String, StationLog, String>.Context context, Collector<String> collector) throws Exception {
                //3.获取状态值
                Long callTime = callTimeValueState.value();
                if (callTime == null) {
                    //说明状态为空，这里该主叫的第一次通话，将通话时间直接存储在状态中
                    callTimeValueState.update(stationLog.callTime);
                } else {
                    //状态中有值，计算两次通话时长
                    long intervalTime = stationLog.callTime - callTime;
                    collector.collect("主叫 ：" + stationLog.callOut + ",最近两次通话间隔时长：" + intervalTime);
                    //4.更新状态
                    callTimeValueState.update(stationLog.callTime);
                }
            }
        });

        result.print();
        env.execute();
    }
}
