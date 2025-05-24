package com.yw.flink.example.javacases.case09_state;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink 状态生存时间TTL 测试
 * 案例:读取Socket中基站通话数据，统计每个主叫通话总时长。
 *
 */
public class Case06_TTL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取socket数据
        DataStreamSource<String> ds = env.socketTextStream("node5", 9999);
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
        KeyedStream<StationLog, String> keyedStream = stationLogDS.keyBy(
                (KeySelector<StationLog, String>) stationLog -> stationLog.callOut);

        //valueState 状态来测试TTL
        keyedStream.map(new RichMapFunction<StationLog, String>() {
            private ValueState<Long> valueState ;
            //注册并获取状态
            @Override
            public void open(Configuration parameters) throws Exception {
                //配置状态TTL
                StateTtlConfig ttlConfig = StateTtlConfig
                        //系统处理数据的时间来计算，而不是事件时间
                        .newBuilder(Time.seconds(10))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .build();

                //注册状态
                ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("ttlState", Long.class);

                //设置TTL
                descriptor.enableTimeToLive(ttlConfig);

                //获取状态
                valueState = getRuntimeContext().getState(descriptor);

            }

            @Override
            public String map(StationLog stationLog) throws Exception {
                //获取当前状态值 - 当前主叫总的通话时长
                Long stateValue = valueState.value();

                if(stateValue==null){
                    valueState.update(stationLog.duration);
                }else{
                    valueState.update(stationLog.duration+stateValue);
                }

                return "当前主叫："+stationLog.callOut+",总通话时长:"+valueState.value();
            }
        }).print();
        env.execute();
    }
}
