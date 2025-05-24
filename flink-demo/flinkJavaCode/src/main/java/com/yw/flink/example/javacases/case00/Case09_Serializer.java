package com.yw.flink.example.javacases.case00;

import com.yw.flink.example.Student;
import com.yw.flink.example.StudentSerializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink 序列化测试
 * @author yangwei
 */
public class Case09_Serializer {
    public static void main(String[] args) throws Exception {
        // 准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().registerTypeWithKryoSerializer(Student.class, StudentSerializer.class);

        DataStreamSource<String> ds = env.socketTextStream("node5", 9999);

        ds.map((MapFunction<String, Object>) s -> {
            String[] slice = s.split(",");
            return new Student(Integer.valueOf(slice[0]), slice[1], Integer.valueOf(slice[2]));
        }).print();

        env.execute();
    }
}
