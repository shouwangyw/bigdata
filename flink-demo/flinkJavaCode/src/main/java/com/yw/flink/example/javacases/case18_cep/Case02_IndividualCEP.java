package com.yw.flink.example.javacases.case18_cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * 简单事件定义模式 ： 设置量词和条件
 */
public class Case02_IndividualCEP {
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
                }).oneOrMore()
                .until(new IterativeCondition<String>() {
                    @Override
                    public boolean filter(String s, Context<String> context) throws Exception {
                        return s.equals("end");
                    }
                });

        //3.模式作用到数据流中
        PatternStream<String> patternStream = CEP.pattern(ds, pattern).inProcessingTime();

        //4.获取匹配结果
        SingleOutputStreamOperator<String> result = patternStream
                .select((PatternSelectFunction<String, String>) map -> {
                    List<String> first = map.get("first");
                    StringBuilder builder = new StringBuilder();
                    for (String s : first) {
                        builder.append(s).append("-");
                    }
                    return builder.substring(0, builder.length() - 1);
                });

        result.print();
        env.execute();
    }
}
