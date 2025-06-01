package com.yw.flink.example.javacases.case18_cep;

import com.yw.flink.example.OrderInfo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Flink CEP - 订单支付超时检测
 * 案例：读取Socket基站用户订单数据，
 * 如果用户在下订单后的一定时间内进行了支付，提示订单支付成功发货。 - 主流
 * 如果用户在下订单后的一定时间内没有支付，提示订单支付超时。- 侧流
 *
 */
public class Case12_PayCEP {
    public static void main(String[] args) throws Exception {
        //1.创建事件流
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //order1,100,1000,create
        KeyedStream<OrderInfo, String> ds = env.socketTextStream("nc_server", 9999)
                .map((MapFunction<String, OrderInfo>) s -> {
                    String[] split = s.split(",");
                    return new OrderInfo(split[0], Double.valueOf(split[1]), Long.valueOf(split[2]), split[3]);
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((SerializableTimestampAssigner<OrderInfo>) (orderInfo, l) -> orderInfo.getOrderTime())
                                .withIdleness(Duration.ofSeconds(5))
                ).keyBy((KeySelector<OrderInfo, String>) OrderInfo::getOrderId);

        //2.定义模式匹配的规则
        Pattern<OrderInfo, OrderInfo> pattern = Pattern.<OrderInfo>begin("first")
                .where(new SimpleCondition<OrderInfo>() {
                    @Override
                    public boolean filter(OrderInfo orderInfo) throws Exception {
                        return orderInfo.getPayState().equals("create");
                    }
                }).followedBy("second").where(new SimpleCondition<OrderInfo>() {
                    @Override
                    public boolean filter(OrderInfo orderInfo) throws Exception {
                        return orderInfo.getPayState().equals("pay");
                    }
                }).within(Time.seconds(20));

        //3.将规则应用到事件流上
        PatternStream<OrderInfo> patternStream = CEP.pattern(ds, pattern);

        //定义outputTag
        OutputTag<String> outputTag = new OutputTag<String>("pay-timeout") {
        };

        //4.获取数据 - 正常数据+超时数据
        SingleOutputStreamOperator<String> result = patternStream.process(new MyPatternProcessFunction(outputTag));

        result.print("订单支付");

        result.getSideOutput(outputTag).print("支付超时");
        env.execute();
    }

    private static class MyPatternProcessFunction extends PatternProcessFunction<OrderInfo,String> implements TimedOutPartialMatchHandler<OrderInfo>{

        private OutputTag<String> outputTag;

        public MyPatternProcessFunction(OutputTag<String> outputTag){
            this.outputTag = outputTag;
        }

        //处理匹配的事件组
        @Override
        public void processMatch(
                Map<String, List<OrderInfo>> map,
                Context context,
                Collector<String> collector) throws Exception {
            //获取订单
            String orderId = map.get("second").get(0).getOrderId();
            collector.collect("订单："+orderId+" 支付成功，待发货！");

        }

        //处理超时事件
        @Override
        public void processTimedOutMatch(
                Map<String, List<OrderInfo>> map,
                Context context) throws Exception {
            String orderID = map.get("first").get(0).getOrderId();
            context.output(outputTag,"订单："+orderID+" 支付超时！");
        }
    }
}
