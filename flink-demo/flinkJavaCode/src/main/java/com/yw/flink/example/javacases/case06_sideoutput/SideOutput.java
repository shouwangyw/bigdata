package com.yw.flink.example.javacases.case06_sideoutput;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Flink 侧输出流测试
 * 案例：Flink读取Socket中通话数据，将成功和不成功的数据信息分别输出。
 */
public class SideOutput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * 001,186,187,success,1000,10
         * 002,187,186,fail,2000,20
         * 003,186,188,success,3000,30
         * 004,188,186,success,4000,40
         * 005,188,187,busy,5000,50
         */
        DataStreamSource<String> ds = env.socketTextStream("node5", 9999);

        //定义侧输出流的tag标签
        OutputTag<String> outputTag = new OutputTag<String>("side-output") {
        };


        SingleOutputStreamOperator<String> mainDS = ds.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String line, ProcessFunction<String, String>.Context ctx, Collector<String> collector) throws Exception {
                String[] split = line.split(",");
                String callType = split[3];
                if ("success".equals(callType)) {
                    //通话成功数据放入主流
                    collector.collect(line);
                } else {
                    //通话不成功数据放入侧流
                    ctx.output(outputTag, line);
                }

            }
        });

        //打印主流
        mainDS.print("主流-通话成功数据：");

        //打印侧流
        mainDS.getSideOutput(outputTag).print("侧流-通话不成功数据:");

        env.execute();


    }
}
