package com.yw.flink.example.javacases.case02_transformation;

import com.yw.flink.example.StationLog;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class Case05_Aggreagtions {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<StationLog> list = Arrays.asList(
                new StationLog("sid1", "18600000000", "18600000001", "success", System.currentTimeMillis(), 120L),
                new StationLog("sid1", "18600000001", "18600000002", "fail", System.currentTimeMillis(), 30L),
                new StationLog("sid1", "18600000002", "18600000003", "busy", System.currentTimeMillis(), 50L),
                new StationLog("sid1", "18600000003", "18600000004", "barring", System.currentTimeMillis(), 90L),
                new StationLog("sid1", "18600000004", "18600000005", "success", System.currentTimeMillis(), 300L)
        );

        DataStreamSource<StationLog> ds = env.fromCollection(list);
        KeyedStream<StationLog, String> ds2 = ds.keyBy(one -> one.sid);

//        SingleOutputStreamOperator<StationLog> sumResult = ds2.sum("duration");
//        SingleOutputStreamOperator<StationLog> result = ds2.min("duration");
//        SingleOutputStreamOperator<StationLog> result = ds2.minBy("duration");
//        SingleOutputStreamOperator<StationLog> result = ds2.max("duration");
        SingleOutputStreamOperator<StationLog> result = ds2.maxBy("duration");
        result.print();
        env.execute();


    }
}
