package com.yw.flink.example.javacases.case03_richfunc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

/**
 * Flink 函数接口
 */
public class Case01_CommFun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.socketTextStream("node5", 9999);
        SingleOutputStreamOperator<String> result = ds.map(new MapFunction<String, String>() {
            @Override
            public String map(String line) throws Exception {
                //001,186,187,busy,1000,10
                String[] split = line.split(",");
                String sid = split[0];
                String callOut = split[1];
                String callIn = split[2];
                String callType = split[3];
                Long callTime = Long.valueOf(split[4]);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String time = sdf.format(callTime);
                long duration = Long.parseLong(split[5]);
                return "基站：" + sid + "，主叫：" + callOut + "，被叫：" + callIn + "，呼叫类型：" + callType + "，呼叫时间" + time + "，呼叫时长" + duration;
            }
        });
        result.print();
        env.execute();
    }
}
