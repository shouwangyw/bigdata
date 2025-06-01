package com.yw.flink.example.javacases.case18_cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink CEP - notNext 连续策略
 */
public class Case06_NotNext {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.定义事件流
        DataStreamSource<String> ds = env.socketTextStream("nc_server", 9999);

        //2.定义Pattern模式
        Pattern<String, String> pattern = Pattern.<String>begin("first")
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        return s.startsWith("a");
                    }
                }).notNext("second").where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        return s.startsWith("c");
                    }
                });

        //3.模式作用到数据流中
        PatternStream<String> patternStream = CEP.pattern(ds, pattern).inProcessingTime();

        //4.获取匹配结果
        SingleOutputStreamOperator<String> result = patternStream
                .select((PatternSelectFunction<String, String>) map -> map.get("first").get(0));

        result.print();
        env.execute();
    }
}
