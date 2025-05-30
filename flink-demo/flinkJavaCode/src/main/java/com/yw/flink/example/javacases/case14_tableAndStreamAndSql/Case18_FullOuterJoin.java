
package com.yw.flink.example.javacases.case14_tableAndStreamAndSql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Flink Table API - FullOuterJoin
 * 案例：读取Socket - 8888 数据 形成表 与 读取Socket -9999 数据形成表进行 FullOuterJoin关联
 * socket-8888 : 1,zs,18,1000
 * scoket-9999 : 1,zs,beijing,1000
 */
public class Case18_FullOuterJoin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读取socket-8888中的数据
        SingleOutputStreamOperator<Tuple4<Integer, String, Integer, Long>> personDS = env.socketTextStream("node5", 8888)
                .map((MapFunction<String, Tuple4<Integer, String, Integer, Long>>) s -> {
                    String[] split = s.split(",");
                    return Tuple4.of(Integer.valueOf(split[0]), split[1], Integer.valueOf(split[2]), Long.valueOf(split[3]));
                });

        //读取socket-9999中的数据
        SingleOutputStreamOperator<Tuple4<Integer, String, String, Long>> addressDS = env.socketTextStream("node5", 9999)
                .map((MapFunction<String, Tuple4<Integer, String, String, Long>>) s -> {
                    String[] split = s.split(",");
                    return Tuple4.of(Integer.valueOf(split[0]), split[1], split[2], Long.valueOf(split[3]));
                });

        //将DS转换成Table
        Table personTable = tableEnv.fromDataStream(personDS,
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.STRING())
                        .column("f2", DataTypes.INT())
                        .column("f3", DataTypes.BIGINT())
                        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(f3,3)")
                        .watermark("rowtime", "rowtime - INTERVAL '2' SECONDS")
                        .build()
        ).as("left_id", "left_name", "age", "left_dt", "left_rowtime");


        //1,zs,beijing,1000
        Table addressTbl = tableEnv.fromDataStream(addressDS,
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.STRING())
                        .column("f2", DataTypes.STRING())
                        .column("f3", DataTypes.BIGINT())
                        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(f3,3)")
                        .watermark("rowtime", "rowtime - INTERVAL '2' SECONDS")
                        .build()
        ).as("right_id", "right_name", "city", "right_dt", "right_rowtime");

        personTable.printSchema();
        addressTbl.printSchema();

        //fullouteJoin
        Table result = personTable.fullOuterJoin(addressTbl, $("left_id").isEqual($("right_id")))
                .select(
                        $("left_id"),
                        $("left_name"),
                        $("age"),
                        $("right_id"),
                        $("right_name"),
                        $("city")

                );

        result.execute().print();
    }
}
