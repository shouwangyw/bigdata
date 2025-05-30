package com.yw.flink.example.javacases.case14_tableAndStreamAndSql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * Flink Table API - 创建查询时态表数据
 */
public class Case19_TableApiTemporalJoin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //设置watermark自动推进
        tableEnv.getConfig().set("table.exec.source.idle-timeout","50000");

        //读取socket-8888 -主流数据：p_002,1000
        SingleOutputStreamOperator<Tuple2<String, Long>> leftInfo = env.socketTextStream("node5", 8888)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        return Tuple2.of(s.split(",")[0], Long.valueOf(s.split(",")[1]));
                    }
                });

        //将左表转换成Table
        Table leftTable = tableEnv.fromDataStream(leftInfo,
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.BIGINT())
                        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(f1,3)")
                        .watermark("rowtime", "rowtime - INTERVAL '5' SECONDS")
                        .build()).as("left_product_id", "left_dt", "left_rowtime");

        //读取时态数据 socket-9999:1000,p_001,电脑,3.0
        SingleOutputStreamOperator<Tuple4<Long, String, String, Double>> rightInfo = env.socketTextStream("node5", 9999)
                .map(new MapFunction<String, Tuple4<Long, String, String, Double>>() {
                    @Override
                    public Tuple4<Long, String, String, Double> map(String s) throws Exception {
                        String[] split = s.split(",");
                        return Tuple4.of(
                                Long.valueOf(split[0]),
                                split[1],
                                split[2],
                                Double.valueOf(split[3])
                        );
                    }
                });

        //将时态表转换成Table
        Table rightTable = tableEnv.fromDataStream(rightInfo,
                Schema.newBuilder()
                        .column("f0", DataTypes.BIGINT())
                        .column("f1", DataTypes.STRING())
                        .column("f2", DataTypes.STRING())
                        .column("f3", DataTypes.DOUBLE())
                        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(f0,3)")
                        .watermark("rowtime", "rowtime - INTERVAL '5' SECONDS")
                        .build()).as("right_update_time","right_product_id","right_product_name","right_price",
                "right_rowtime");

        //创建时态表函数
        TemporalTableFunction temporalTableFunction = rightTable.createTemporalTableFunction($("right_rowtime"), $("right_product_id"));

        //注册时态表函数
        tableEnv.createTemporarySystemFunction("temporalTableFunction",temporalTableFunction);

        //使用时态表函数查询时态表数据
        Table result = leftTable.joinLateral(
                call("temporalTableFunction", $("left_rowtime")),
                $("left_product_id").isEqual($("right_product_id"))
        ).select(
                $("left_product_id"),
                $("left_dt"),
                $("right_product_name"),
                $("right_price")
        );

        result.execute().print();
    }
}
