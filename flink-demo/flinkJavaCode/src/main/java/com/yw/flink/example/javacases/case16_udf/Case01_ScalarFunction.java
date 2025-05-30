package com.yw.flink.example.javacases.case16_udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * Flink Table API & SQL - 自定义函数 - Scalar Function
 * 案例：读取Kafka 中基站日志数据，通过自定义函数输出信息
 */
public class Case01_ScalarFunction {
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

        //注册自定义函数
        tableEnv.createTemporarySystemFunction("my_concat", ConcatStringUDF.class);

        //table api
        Table result = tableEnv.from("station_log_tbl")
                .select(
                        $("sid"),
                        call("my_concat", $("call_out"), $("call_in"))
                );

        result.execute().print();


        //SQL 中使用自定义函数
//        Table result = tableEnv.sqlQuery("select sid,my_concat(call_out,call_in,call_type,duration) as info from station_log_tbl");
//        result.execute().print();

    }

    /**
     *
     * 1.自定义标量函数必须继承 ScalarFunction 抽象类
     * 2.类必须定义成public,不允许使用非静态内部类或匿名类
     *
     */
    public static class ConcatStringUDF extends ScalarFunction {
        public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object... args){
            StringBuilder builder = new StringBuilder();
            for (Object arg : args) {
                builder.append(arg.toString()).append("|");
            }
            return builder.substring(0,builder.toString().length()-1);
        }
    }
}
