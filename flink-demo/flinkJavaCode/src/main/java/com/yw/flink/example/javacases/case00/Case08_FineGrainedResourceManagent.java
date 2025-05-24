package com.yw.flink.example.javacases.case00;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink 细粒度资源测试
 */
public class Case08_FineGrainedResourceManagent {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        //创建SSG对象指定使用的Slot资源
//        SlotSharingGroup ssg = SlotSharingGroup.newBuilder("ssg")
//                .setCpuCores(0.1)
//                .setTaskHeapMemoryMB(20)
//                .build();

        SingleOutputStreamOperator<String> ds1 = env.socketTextStream("node5", 9999).slotSharingGroup("ssg");

        SingleOutputStreamOperator<String> ds3 = ds1.flatMap((String line, Collector<String> collector) -> {
            String[] words = line.split(",");
            for (String word : words) {
                collector.collect(word);
            }
        }).returns(Types.STRING).slotSharingGroup("ssg");

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds4 =
                ds3.map(word -> Tuple2.of(word, 1)).returns(Types.TUPLE(Types.STRING, Types.INT)).slotSharingGroup("ssg");

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds5 =
                ds4.keyBy(tp -> tp.f0).sum(1).slotSharingGroup("ssg");

        ds5.print().slotSharingGroup("ssg");

        //注册SSG对象
//        env.registerSlotSharingGroup(ssg);

        env.execute();
    }
}
