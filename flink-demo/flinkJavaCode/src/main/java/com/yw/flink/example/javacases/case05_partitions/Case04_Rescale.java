package com.yw.flink.example.javacases.case05_partitions;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Arrays;
import java.util.List;

/**
 * Flink 重缩放分区策略
 */
public class Case04_Rescale {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Object> ds = env.addSource(new RichParallelSourceFunction<Object>() {
            @Override
            public void run(SourceContext<Object> ctx) throws Exception {
                List<String> list1 = Arrays.asList("a", "b", "c", "d", "e", "f");
                List<Integer> list2 = Arrays.asList(1, 2, 3, 4, 5, 6);
                //getRuntimeContext().getIndexOfThisSubtask() : 获取subtask的号，从0开始
                if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
                    for (String s : list1) {
                        ctx.collect(s);
                    }
                }
                if (getRuntimeContext().getIndexOfThisSubtask() == 1) {
                    for (Integer s : list2) {
                        ctx.collect(s);
                    }
                }
            }
            @Override
            public void cancel() {
            }
        });

        //使用重缩放分区
        ds.rescale().print().setParallelism(4);
        env.execute();
    }
}
