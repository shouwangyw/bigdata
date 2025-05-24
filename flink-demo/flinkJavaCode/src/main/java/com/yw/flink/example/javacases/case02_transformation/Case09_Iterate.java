package com.yw.flink.example.javacases.case02_transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 用于迭代计算，迭代计算场景，注重的是某块业务逻辑一直迭代计算
 */
public class Case09_Iterate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.socketTextStream("node5", 9999);
        SingleOutputStreamOperator<Integer> ds2 = ds.map(one -> Integer.valueOf(one));

        //创建迭代流
        IterativeStream<Integer> iterate = ds2.iterate();

        //定义迭代体 ，就是我们看中的多次迭代计算的逻辑
        SingleOutputStreamOperator<Integer> minusOne = iterate.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer integer) throws Exception {
                System.out.println("迭代体中进来的数据为 = "+integer);
                return integer - 1;
            }
        });

        //定义迭代条件 ，就是什么时候需要继续迭代计算
        SingleOutputStreamOperator<Integer> cn = minusOne.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value > 0;
            }
        });

        //对迭代流应用条件
        iterate.closeWith(cn);
        iterate.print();
        env.execute();

    }
}
