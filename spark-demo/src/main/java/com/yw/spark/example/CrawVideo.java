package com.yw.spark.example;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yw.spark.example.po.CourseInfo;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author yangwei
 */
public class CrawVideo {
    private static final int MAX_THREADS = 12;
    private static final ThreadPoolExecutor EXECUTOR = new ThreadPoolExecutor(MAX_THREADS, MAX_THREADS,
            60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder().setNameFormat("CrawVideoThreadPool-%d").build());
    private static final String MYSQL_URL = "jdbc:mysql://node01:3306/kaikeba";
    private static final String MYSQL_USR = "root";
    private static final String MYSQL_PWD = "123456";
    private static final String PATH = "/Volumes/TOSHIBA\\ EXT/kakeba";

    public static void main(String[] args) throws Exception {
        try (Connection conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USR, MYSQL_PWD);
             Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery("select * from craw_video_info where has_done = 0 limit 1000")) {
            while (rs.next()) {
                final Integer id = rs.getInt(1);
                final String courseName = rs.getString(3);
                final String chapterName = rs.getString(5);
                final String groupName = rs.getString(11);
                final String contentName = rs.getString(13);
                final String videoUrl = rs.getString(17);
            }
        }


        if (count.get() >= MAX_THREADS) {
            TimeUnit.MINUTES.sleep(1);
            count.set(0);
        }

//        Integer courseId = 213711; // “门徒计划”-Web前端方向
//        final Map<Integer, String> courseInfo = Utils.getChapterList(courseId);
//        courseInfo.forEach((k, v) -> {
//            final String json = Utils.getDownloadJson(courseId, k);
//
//            System.out.println(json);
//            download(PATH + v + "/", json);
//        });
    }



    private static AtomicInteger count = new AtomicInteger(0);

    private static void download(String path, String json) {
        try {
            Map<String, String> titleAndKey = Utils.getTitleAndKey(json);

            for (Map.Entry<String, String> e : titleAndKey.entrySet()) {
                String playUrl = Utils.getPlayUrlFromDetail(e.getValue());
                String dir = "";
                if (StringUtils.contains(e.getKey(), "/")) {
                    dir = e.getKey().split("/")[0];
                }
                if (!new File(path.replace("\\", "") + dir).exists()) {
                    Utils.exec("mkdir -p " + (path + dir));
                }
                final String fileName = (path + e.getKey() + ".mp4").replace("(", "\\(").replace(")", "\\)");
                File file = new File(fileName.replace("\\", ""));
                final String command = getDownloadCommand(playUrl, fileName);
                if (!file.exists()) {
                    EXECUTOR.execute(() -> Utils.exec(command));
                    count.incrementAndGet();
                }

                if (count.get() >= MAX_THREADS) {
                    TimeUnit.MINUTES.sleep(1);
                    count.set(0);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("ERROR: " + json);
        }
    }

    private static void download2(String path, String title, String playUrl) {
        final String fileName = (path + title + ".mp4").replace("(", "\\(").replace(")", "\\)");
        File file = new File(fileName.replace("\\", ""));
        final String command = getDownloadCommand(playUrl, fileName);
        if (!file.exists()) {
            EXECUTOR.execute(() -> Utils.exec(command));
            count.incrementAndGet();
        }
    }

    private static String getDownloadCommand(String url, String fileName) {
        // ffmpeg -allowed_extensions ALL -i "https://v.baoshiyun.com/resource/media-853926905577472/lud/f3a2a0fd4d5241c4916c631636d36f2f.m3u8?MtsHlsUriToken=b3aa93f493494b078d5dd2be84fa5c297970c21158994582a92b3be49e2a53a0" -c copy /Volumes/G/kaikeba/spark/SparkStreaming01/8_poll方式消费flume数据.mp4

        return "/usr/local/bin/ffmpeg -allowed_extensions ALL -i \"" + url + "\" -c copy " + fileName;
    }
}
