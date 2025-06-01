package com.yw.flink.example.javacases.case09_state;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink MapState 映射状态测试
 * 案例：读取基站日志数据，统计主叫号码呼出的全部被叫号码及对应被叫号码通话总时长。
 * 001,186,187,busy,1000,10
 * 002,187,186,fail,2000,20
 * 003,186,188,busy,3000,30
 * 004,187,186,busy,4000,40
 * 005,186,187,busy,5000,50
 */
public class Case05_MapState {
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
        KeyedStream<StationLog, String> keyedStream = stationLogDS.keyBy(
                (KeySelector<StationLog, String>) stationLog -> stationLog.callOut);

        keyedStream.map(new RichMapFunction<StationLog, String>() {
            private MapState<String, Long> mapState ;
            //状态注册及获取
            @Override
            public void open(Configuration parameters) throws Exception {
                //注册状态
                MapStateDescriptor<String, Long> mapstateDescriptor = new MapStateDescriptor<>("mapstate", String.class, Long.class);
                //获取状态
                mapState = getRuntimeContext().getMapState(mapstateDescriptor);
            }

            @Override
            public String map(StationLog stationLog) throws Exception {
                //获取当前主叫号码对应的被叫号码
                String callIn = stationLog.callIn;
                //获取当前被叫通话时长
                Long duration = stationLog.duration;

                if(mapState.contains(callIn)){
                    //包含当前被叫
                    mapState.put(callIn,mapState.get(callIn)+duration);
                }else{
                    //不包含
                    mapState.put(callIn,duration);
                }

                //组织返回的数据
                StringBuilder info = new StringBuilder();
                for(String key : mapState.keys()){
                    Long currentKeyDuration = mapState.get(key);
                    info.append("被叫：").append(key).append(",通话总时长：").append(currentKeyDuration).append("->");
                }
                return "主叫："+stationLog.callOut+",通话信息"+info;
            }
        }).print();
        env.execute();

        
    }
}
