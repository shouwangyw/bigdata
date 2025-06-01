package com.yw.flink.example.javacases.case18_cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * Flink CEP - 循环模式中的连续性
 * consecutive ： 严格邻近关系
 */
public class Case08_Consecutive {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.定义事件流
        DataStreamSource<String> ds = env.socketTextStream("nc_server", 9999);

        //2.定义Pattern模式
        Pattern<String, String> pattern = Pattern.<String>begin("first").where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        return s.startsWith("a");
                    }
                }).oneOrMore() //默认多个事件之间使用宽松邻近策略
                .allowCombinations();//设置多个时间之间使用非确定宽松邻近策略
//                .consecutive();//设置多个事件之间使用严格邻近策略

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
