package com.yw.flink.example.javacases.case20_optimize;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * SQL中设置TTL
 */
public class Case10_TTL {
    public static void main(String[] args) {
        //获取DataStream的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取Table API的运行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //设置状态保存时间，默认为0，表示不清理状态，这里设置5s
        tableEnv.getConfig().set("table.exec.state.ttl", "5000");

        SingleOutputStreamOperator<StationLog> ds = env.socketTextStream("nc_server", 9999)
                .map((MapFunction<String, StationLog>) s -> {
                    String[] split = s.split(",");
                    return new StationLog(split[0], split[1], split[2], split[3], Long.valueOf(split[4]), Long.valueOf(split[5]));
                });

        //将DataStream转换成Table
        tableEnv.createTemporaryView("station_tbl", ds);

        //通过SQL统计通话时长信息
        Table result = tableEnv.sqlQuery("select sid,sum(duration) as total_duration from station_tbl group by sid");

        //打印输出
        result.execute().print();
    }
}
