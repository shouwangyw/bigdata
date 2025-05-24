package com.yw.flink.example.javacases.case01_source;

import com.yw.flink.example.StationLog;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * Flink 集合Source
 */
public class Case02_CollectionSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //准备集合
        ArrayList<StationLog> stationLogs = new ArrayList<>();
        stationLogs.add(new StationLog("001", "186", "187", "busy", 1000L, 0L));
        stationLogs.add(new StationLog("002", "187", "186", "fail", 1000L, 0L));
        stationLogs.add(new StationLog("003", "188", "189", "success", 1000L, 0L));
        stationLogs.add(new StationLog("004", "189", "187", "fail", 1000L, 0L));
        stationLogs.add(new StationLog("005", "187", "186", "busy", 1000L, 0L));

//        env.fromCollection(Arrays.asList(new StationLog()))
        DataStreamSource<StationLog> result = env.fromCollection(stationLogs);
        result.print();
        env.execute();
    }
}
