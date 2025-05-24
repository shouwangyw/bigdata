package com.yw.flink.example.javacases.case09_state;

import com.yw.flink.example.PersonInfo;
import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Flink 广播状态 测试
 * 案例：读取两个socket数据，8888，9999端口
 * A流为基站日志数据，B流为通话的人的基本信息
 * B流广播与A流进行关联，输出通话的详细信息
 */
public class Case08_BroadCast {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取基站通话日志数据
        SingleOutputStreamOperator<StationLog> mainDS = env.socketTextStream("node5", 8888).map((MapFunction<String, StationLog>) line -> {
            String[] split = line.split(",");
            return new StationLog(split[0], split[1], split[2], split[3], Long.valueOf(split[4]), Long.valueOf(split[5]));

        });


        //获取通话人员的基本信息 187,张三,北京
        SingleOutputStreamOperator<PersonInfo> personInfoDS = env.socketTextStream("node5", 9999).map((MapFunction<String, PersonInfo>) line -> {
            String[] split = line.split(",");
            return new PersonInfo(split[0], split[1], split[2]);
        });

        //注册广播状态
        MapStateDescriptor<String, PersonInfo> mapStateDescriptor = new MapStateDescriptor<>("mapState", String.class, PersonInfo.class);

        //对人员信息进行广播
        BroadcastStream<PersonInfo> broadcastStream = personInfoDS.broadcast(mapStateDescriptor);

        //两流关联
        SingleOutputStreamOperator<String> result = mainDS.connect(broadcastStream).process(new BroadcastProcessFunction<StationLog, PersonInfo, String>() {
            /**
             * 处理主流数据，来一条处理一条
             * 001,186,187,busy,1000,10
             * 002,187,186,fail,2000,20
             */
            @Override
            public void processElement(StationLog stationLog, BroadcastProcessFunction<StationLog, PersonInfo, String>.ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
                ReadOnlyBroadcastState<String, PersonInfo> broadCastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
                PersonInfo callOutPersonInfo = broadCastState.get(stationLog.getCallOut());
                PersonInfo callInPersonInfo = broadCastState.get(stationLog.getCallIn());
                String callOutName = callOutPersonInfo != null ? callOutPersonInfo.getName() : "无姓名";
                String callOutCity = callOutPersonInfo != null ? callOutPersonInfo.getCity() : "无城市";
                String callInName = callInPersonInfo != null ? callInPersonInfo.getName() : "无姓名";
                String callInCity = callInPersonInfo != null ? callInPersonInfo.getCity() : "无城市";

                collector.collect("主叫姓名：" + callOutName + ",主叫城市" + callOutCity + "," + "被叫姓名：" + callInName + ",被叫城市：" + callInCity + "，通话时长:" + stationLog.getDuration());
            }

            //处理广播流的数据，来一条处理一条
            @Override
            public void processBroadcastElement(PersonInfo personInfo, BroadcastProcessFunction<StationLog, PersonInfo, String>.Context context, Collector<String> collector) throws Exception {
                //获取广播状态
                BroadcastState<String, PersonInfo> broadState = context.getBroadcastState(mapStateDescriptor);
                broadState.put(personInfo.getPhoneNum(), personInfo);

            }
        });

        result.print();
        env.execute();
    }
}
