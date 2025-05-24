package com.yw.flink.example.javacases.case02_transformation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Arrays;

public class Case08_Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> ds1 = env.fromCollection(Arrays.asList(
                Tuple2.of("a", 1),
                Tuple2.of("b", 2),
                Tuple2.of("c", 3),
                Tuple2.of("d", 4)
        ));
        DataStreamSource<String> ds2 = env.fromCollection(Arrays.asList("aa", "bb", "cc", "dd"));

        ConnectedStreams<Tuple2<String, Integer>, String> connect = ds1.connect(ds2);

        SingleOutputStreamOperator<String> result = connect.map(new CoMapFunction<Tuple2<String, Integer>, String, String>() {
            //处理第一个流数据
            @Override
            public String map1(Tuple2<String, Integer> tp) throws Exception {
                return tp.f0 + "-" + tp.f1;
            }

            //处理第二个流中的数据
            @Override
            public String map2(String s) throws Exception {
                return s + "=";
            }
        });

        result.print();
        env.execute();
    }
}
