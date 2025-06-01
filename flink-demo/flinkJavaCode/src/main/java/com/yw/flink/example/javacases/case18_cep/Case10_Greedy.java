package com.yw.flink.example.javacases.case18_cep;

import com.yw.flink.example.LoginInfo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Flink CEP - greedy 贪婪模式使用
 * 案例：读取socket用户登录数据，当用户登录成功后，输出用户登录成功前的所有登录状态
 */
public class Case10_Greedy {
    public static void main(String[] args) throws Exception {
        //准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.定义事件流 ,uid1,zs,1000,fail1
        KeyedStream<LoginInfo, String> ds = env.socketTextStream("nc_server", 9999)
                .map((MapFunction<String, LoginInfo>) s -> {
                    String[] split = s.split(",");
                    return new LoginInfo(split[0], split[1], Long.valueOf(split[2]), split[3]);
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<LoginInfo>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((SerializableTimestampAssigner<LoginInfo>) (loginInfo, l) -> loginInfo.getLoginTime())
                                .withIdleness(Duration.ofSeconds(5))
                )
                .keyBy((KeySelector<LoginInfo, String>) LoginInfo::getUid);

        //2.定义模式匹配规则
        Pattern<LoginInfo, LoginInfo> pattern = Pattern.<LoginInfo>begin("first")
                .where(new SimpleCondition<LoginInfo>() {
                    @Override
                    public boolean filter(LoginInfo loginInfo) throws Exception {
                        return loginInfo.getLoginState().startsWith("fail");
                    }
                }).oneOrMore()
                .greedy()
                .followedBy("second").where(new SimpleCondition<LoginInfo>() {
                    @Override
                    public boolean filter(LoginInfo loginInfo) throws Exception {
                        return "success".equals(loginInfo.getLoginState());
                    }
                });
        //3.将匹配规则应用到事件流中
        PatternStream<LoginInfo> patternStream = CEP.pattern(ds, pattern);

        //4.获取匹配数据
        SingleOutputStreamOperator<String> result = patternStream.process(new PatternProcessFunction<LoginInfo, String>() {
            @Override
            public void processMatch(
                    Map<String, List<LoginInfo>> map,
                    Context context,
                    Collector<String> collector) throws Exception {
                List<LoginInfo> first = map.get("first");
                List<LoginInfo> second = map.get("second");
                //获取用户
                String uid = first.get(0).getUid();
                Long successTime = second.get(0).getLoginTime();

                //登录成功前的状态
                StringBuilder builder = new StringBuilder();
                for (LoginInfo loginInfo : first) {
                    builder.append(loginInfo.getLoginState()).append("-");
                }
                collector.collect("用户：" + uid + " 在" + successTime + "登录成功，登录前的状态：" + builder.substring(0, builder.length() - 1));
            }
        });

        result.print();
        env.execute();
    }
}
