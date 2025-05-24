package com.yw.flink.example.javacases.case05_partitions;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Arrays;
import java.util.List;

/**
 * Flink ForwardTest
 */
public class Case07_Forward {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Integer> ds1 = env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
                for (Integer one : list) {
                    int index = getRuntimeContext().getIndexOfThisSubtask();
                    if (one % 2 == 0 && index == 0) {
                        ctx.collect(one);
                    }
                    if (one % 2 != 0 && index == 1) {
                        ctx.collect(one);
                    }
                }

            }

            @Override
            public void cancel() {

            }
        });

        ds1.print("ds1");
        SingleOutputStreamOperator<String> ds2 = ds1.forward().map(one -> one + "~");
        ds2.print("ds2");
        env.execute();
    }
}
