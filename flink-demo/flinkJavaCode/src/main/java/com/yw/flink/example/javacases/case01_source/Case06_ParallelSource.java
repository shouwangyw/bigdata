package com.yw.flink.example.javacases.case01_source;

import com.yw.flink.example.StationLog;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * Java - Flink 自定义Source ,实现ParalleSourceFunction接口
 * sid: 基站id
 * callOut : 主叫号码
 * callIn : 被叫号码
 * callType : 通话类型，fail busy barring success
 * callTime : 呼叫时间
 * duration : 通话时长
 */
public class Case06_ParallelSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource ds = env.addSource(new ParallelSourceFunction<StationLog>() {
            Boolean flag = true;

            @Override
            public void run(SourceContext<StationLog> ctx) throws Exception {
                Random random = new Random();
                String[] callTypes = {"fail", "success", "barring", "busy"};
                while (flag) {
                    String sid = "sid_" + random.nextInt(10);
                    String callOut = "1811234" + (random.nextInt(9000) + 1000);
                    String callIn = "1915678" + (random.nextInt(9000) + 1000);
                    String callType = callTypes[random.nextInt(4)];
                    long callTime = System.currentTimeMillis();
                    Long duration = (long) random.nextInt(50);
                    ctx.collect(new StationLog(sid, callOut, callIn, callType, callTime, duration));
                    Thread.sleep(2000);
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        });
        ds.print();
        env.execute();
    }
}
