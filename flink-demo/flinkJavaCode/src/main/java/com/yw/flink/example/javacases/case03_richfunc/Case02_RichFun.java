package com.yw.flink.example.javacases.case03_richfunc;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;

public class Case02_RichFun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.socketTextStream("node5", 9999);
        SingleOutputStreamOperator<String> result = ds.map(new RichMapFunction<String, String>(){
            Connection conn = null;
            PreparedStatement pst = null;

            ResultSet rst = null;

            //在map方法执行之前执行，用于初始化资源
            @Override
            public void open(Configuration parameters) throws Exception {
                conn = DriverManager.getConnection("jdbc:mysql://node2:3306/mydb?useSSL=false", "root", "123456");
                //准备查询语句
                pst = conn.prepareStatement("select phone_num ,name ,city from person_info where phone_num = ?");
            }

            @Override
            public String map(String line) throws Exception {
                //001,186,187,busy,1000,10
                String[] split = line.split(",");
                String sid = split[0];
                String callOut = split[1];
                String callIn = split[2];
                String callType = split[3];
                Long callTime = Long.valueOf(split[4]);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String time = sdf.format(callTime);
                long duration = Long.parseLong(split[5]);

                String callOutName = "";
                String callInName = "";
                //查询主叫手机号对应的姓名
                pst.setString(1, callOut);
                rst = pst.executeQuery();
                while (rst.next()) {
                    callOutName = rst.getString("name");
                }

                //查询被叫手机号对应的姓名
                pst.setString(1, callIn);
                rst = pst.executeQuery();
                while (rst.next()) {
                    callInName = rst.getString("name");
                }
                return "基站：" + sid + "，主叫：" + callOutName + "，被叫：" + callInName + "，呼叫类型：" + callType + "，呼叫时间" + time + "，呼叫时长" + duration;
            }

            //用于在map方法执行之后执行，用于清理资源
            @Override
            public void close() throws Exception {
                rst.close();
                pst.close();
                conn.close();
            }
        });

        result.print();
        env.execute();
    }
}
