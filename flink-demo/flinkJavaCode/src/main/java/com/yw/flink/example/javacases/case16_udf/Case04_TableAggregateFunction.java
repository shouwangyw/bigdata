package com.yw.flink.example.javacases.case16_udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * Flink Table API - 表聚合函数
 * 案例：读取Kafka 基站日志数据，对每个基站输出通话时长top2
 */
public class Case04_TableAggregateFunction {
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

        //注册自定义表聚合函数
        tableEnv.createTemporarySystemFunction("my_top2Agg", Top2DurationTableUDAF.class);

        //表聚合函数只能通过Table api 调用
        Table result = tableEnv.from("station_log_tbl")
                .groupBy($("sid"))
                .flatAggregate(call("my_top2Agg", $("duration")).as("dur", "rank"))
                .select($("sid"), $("dur"), $("rank"));

        result.execute().print();
    }

    /**
     * TableAggregateFunction<T,ACC>
     * T:聚合后结果
     * ACC:累加器类型
     */
    public class Top2DurationTableUDAF extends TableAggregateFunction<Tuple2<Long, Integer>, Tuple2<Long, Long>> {

        /**
         * 初始化累加器
         *
         * @return
         */
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return Tuple2.of(Long.MIN_VALUE, Long.MIN_VALUE);
        }

        /**
         * 根据传入的duartion 组织通话时长最大的两个数据
         */
        public void accumulate(Tuple2<Long, Long> acc, Long duration) {
            if (duration > acc.f0) {
                acc.f1 = acc.f0;
                acc.f0 = duration;
            } else if (duration > acc.f1) {
                acc.f1 = duration;
            }
        }

        /**
         * 返回结果，返回的结果中是通话第一大和第二大的值
         */
        public void emitValue(Tuple2<Long, Long> acc, Collector<Tuple2<Long, Integer>> out) {
            if (acc.f0 != Long.MIN_VALUE) {
                out.collect(Tuple2.of(acc.f0, 1));
            }
            if (acc.f1 != Long.MIN_VALUE) {
                out.collect(Tuple2.of(acc.f1, 2));
            }
        }
    }
}
