package com.yw.flink.example.javacases.case09_state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Flink OperatorState 算子状态测试
 * 案例：读取socket基站日志数据，每隔3条打印到控制台
 * 001,186,187,busy,1000,10
 * 002,187,186,fail,2000,20
 * 003,186,188,busy,3000,30
 * 004,187,186,busy,4000,40
 * 005,186,187,busy,5000,50
 */
public class Case07_OperatorState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //开启checkpoint
        env.enableCheckpointing(5000);
        //设置并行度为2
        env.setParallelism(2);
        //读取socket数据
        DataStreamSource<String> ds = env.socketTextStream("nc_server", 9999);
        //将数据每3条拼接打印，这里需要通过实现checkpointFunction接口来保证数据处理一致性
        ds.map(new MyRichMapAndCheckpointFunction()).print();
        env.execute();
    }

    private static class MyRichMapAndCheckpointFunction extends RichMapFunction<String, String> implements CheckpointedFunction {

        //定义的算子状态
        private ListState<String> listState;

        //本地集合，用于存储写出的数据，每3条写出
        private List<String> stationLogList = new ArrayList<>();

        /**
         * 来一条数据如何处理
         */
        @Override
        public String map(String line) throws Exception {

            //当集合达到3条数据后，拼接写出到控制台
            if (stationLogList.size() == 3) {
                StringBuilder info = new StringBuilder();
                for (String s : stationLogList) {
                    info.append(s).append(" | ");
                }
                stationLogList.clear();
                return info.toString();
            } else {
                //来一条数据，加入到集合中
                stationLogList.add(line);
                return "当前并行度数据条数为:" + stationLogList.size() + ",不满足写出条件！";
            }
        }

        /**
         * 该方法在checkpoint触发时调用，需要在该方法中准备状态，由checkpoint写出到外部系统
         */
        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            System.out.println("checkpoint要将状态数据持久化了，需要准备状态!!");

            //将算子状态清空
            listState.clear();

            //将本地集合中的数据添加到状态中
            listState.addAll(stationLogList);
        }

        /**
         * 该方法在自定义处理数据业务逻辑执行前执行，用作初始化状态+恢复状态后如何进行业务逻辑处理
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("initializeState 方法调用了，初始化状态和恢复状态了");
            //创建状态描述器
            ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<>("liststate", String.class);
            //获取状态，这里使用状态重分布 - 均衡方式
            listState = context.getOperatorStateStore().getListState(listStateDescriptor);

            if (context.isRestored()) {
                //外部之前存储了状态，状态自动恢复到 listState ，这里就要写如何使用这些状态了
                for (String line : listState.get()) {
                    stationLogList.add(line);
                }
            }
        }
    }

}

