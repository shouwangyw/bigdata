package com.yw.spark.example;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
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

    public static void main(String[] args) throws Exception {
        Integer courseId = 213711; // “门徒计划”-Web前端方向
        final Map<Integer, String> courseInfo = Utils.getChapterList(courseId);
        courseInfo.forEach((k, v) -> {
            final String json = Utils.getDownloadJson(courseId, k);

            System.out.println(json);
//            download(PATH + v + "/", json);
        });
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
                final String command = "/usr/local/bin/ffmpeg -allowed_extensions ALL -i \"" + playUrl + "\" -c copy " + fileName;
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
}
