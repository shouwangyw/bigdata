package com.yw.flink.example.javacases.case16_udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * Flink Table API & SQL - 自定义聚合函数
 * 案例：读取Kafka 基站日志数据形成表 ，自定义聚合函数计算每个基站的平均通话的时长
 */
public class Case03_AggregateFunction {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());
        //设置watermark 自动推进
        tableEnv.getConfig().set("table.exec.source.idle-timeout", "5000");

        tableEnv.executeSql("create table station_log_tbl(" +
                "   sid string," +
                "   call_out string," +
                "   call_in string," +
                "   call_type string," +
                "   call_time bigint," +
                "   duration bigint," +
                "   rowtime as to_timestamp_ltz(call_time,3)," +
                "   watermark for rowtime as rowtime - interval '2' seconds " +
                ") with (" +
                "   'connector'='kafka'," +
                "   'topic'='station_log-topic'," +
                "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id'='testgroup'," +
                "   'scan.startup.mode'='latest-offset'," +
                "   'format'='csv'" +
                ")");

        //注册自定义聚合函数
        tableEnv.createTemporarySystemFunction("my_avg", AvgDurationUDAF.class);

        //使用Table API 调用聚合函数
//        Table result = tableEnv.from("station_log_tbl")
//                .groupBy($("sid"))
//                .select($("sid"), call("my_avg", $("duration")).as("avg_dur"));
//
//        result.execute().print();

        //使用SQL 调用聚合函数
        tableEnv.executeSql("select sid,my_avg(duration) as avg_dur from station_log_tbl group by sid").print();
    }

    /**
     * AggregateFunction<T,ACC>
     * T:最终聚合结果返回的类型
     * ACC:聚合过程中累加器类型
     */
    public class AvgDurationUDAF extends AggregateFunction<Double, Tuple2<Long, Integer>> {

        /**
         * 创建聚合中间结果状态
         */
        @Override
        public Tuple2<Long, Integer> createAccumulator() {
            return Tuple2.of(0L, 0);
        }

        /**
         * 计算过程
         * accumulate
         */
        public void accumulate(Tuple2<Long, Integer> acc, Long duration) {
            //给累加器中的通话时长加上该duration ,给累加器中的通话次数加上1
            acc.f0 += duration;
            acc.f1 += 1;
        }

        /**
         * 最终返回聚合结果
         */
        @Override
        public Double getValue(Tuple2<Long, Integer> acc) {
            if (acc.f1 == 0) {
                return null;
            } else {
                return acc.f0 * 1.0 / acc.f1;
            }
        }
    }
}
