package com.yw.spark.example;

import com.alibaba.fastjson.JSONObject;
import com.yw.spark.example.po.ChapterInfo;
import com.yw.spark.example.po.CourseInfo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yangwei
 */
public class CrawVideo2MySQL {
    public static void main(String[] args) throws Exception {
        final List<CourseInfo> courses = Utils.getCourseList2();
//        for (CourseInfo courseInfo : courses) {
//            System.out.println(courseInfo);
//        }
        try (Connection conn = DriverManager.getConnection("jdbc:mysql://node01:3306/kaikeba", "root", "123456");
             Statement statement = conn.createStatement()) {
            for (CourseInfo courseInfo : courses) {
                String sql = "insert into course_info(course_id, course_name, cover_img_path_web, cover_img_path_mobile, course_icon) values ("+courseInfo.getCourse_id()+", '"+courseInfo.getCourse_name()+"', '"+courseInfo.getCover_img_path_web()+"', '"+courseInfo.getCover_img_path_mobile()+"', '"+courseInfo.getCourse_icon()+"')";

                System.out.println(sql);
                statement.execute(sql);
            }
        }


//        final Connection conn = DriverManager.getConnection("jdbc:mysql://node01:3306/kaikeba", "root", "123456");
//        final Statement statement = conn.createStatement();
//        for (Map.Entry<Integer, String> entry : Utils.getCourseList().entrySet()) {
//            final Integer courseId = entry.getKey();
//            final ResultSet resultSet = statement.executeQuery("SELECT count(*) from course_video_info where course_id = " + courseId);
//            if (resultSet.next() && resultSet.getInt(1) > 0) {
//                continue;
//            }
//            final String courseName = entry.getValue();
//
//            final Map<Integer, String> courseInfo = Utils.getChapterList(courseId);
//            courseInfo.forEach((k, v) -> {
//                final String res = Utils.getDownloadJson(courseId, k);
//                final String json = JSONObject.toJSONString(JSONObject.parseObject(res).getJSONObject("data"));
//                System.out.println(json);
//
//                String sql = "insert into course_video_info(course_id, course_name, json) values(" + courseId + ", '" + courseName + "', '" + json + "')";
//                try {
//                    statement.execute(sql);
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                }
//            });
//        }
//
//        if (statement != null) {
//            statement.close();
//        }
//        if (conn != null) {
//            conn.close();
//        }
    }

//    public static void main(String[] args) throws Exception {
//        Map<String, List<ChapterInfo>> map = new ConcurrentHashMap<>();
//        try (Connection conn = DriverManager.getConnection("jdbc:mysql://node01:3306/kaikeba", "root", "123456");
//             Statement statement = conn.createStatement();
//             ResultSet resultSet = statement.executeQuery("SELECT * from course_video_info")) {
//            while (resultSet.next()) {
//                final String courseName = resultSet.getString(3);
//                String json = resultSet.getString(4);
//                ChapterInfo chapterInfo;
//                try {
//                    chapterInfo = JSONObject.parseObject(json, ChapterInfo.class);
//                } catch (Exception e) {
//                    json = json.replace("、", "_").replace("\\", "_");
//                    chapterInfo = JSONObject.parseObject(json, ChapterInfo.class);
//                }
//                if (map.containsKey(courseName)) {
//                    map.get(courseName).add(chapterInfo);
//                } else {
//                    ArrayList<ChapterInfo> list = new ArrayList<>();
//                    list.add(chapterInfo);
//                    map.put(courseName, list);
//                }
//            }
//        }
//
//        List<Integer> existContentIds = new LinkedList<>();
//        try (Connection conn = DriverManager.getConnection("jdbc:mysql://node01:3306/kaikeba", "root", "123456");
//             Statement statement = conn.createStatement();
//             ResultSet resultSet = statement.executeQuery("SELECT content_id from craw_video_info")){
//            while (resultSet.next()) {
//                existContentIds.add(resultSet.getInt(1));
//            }
//        }
//
//        try (Connection conn = DriverManager.getConnection("jdbc:mysql://node01:3306/kaikeba", "root", "123456");
//             Statement statement = conn.createStatement()) {
//            for (Map.Entry<String, List<ChapterInfo>> entry : map.entrySet()) {
//                final String courseName = entry.getKey();
//                final List<ChapterInfo> chapterInfos = entry.getValue();
//                for (ChapterInfo chapterInfo : chapterInfos) {
//                    final Integer courseId = chapterInfo.getCourse_id();
//                    final Integer chapterId = chapterInfo.getChapter_id();
//                    final String chapterName = chapterInfo.getChapter_name();
//                    final Integer chapterRankNum = chapterInfo.getRank_num();
//                    for (SectionInfo sectionInfo : chapterInfo.getSection_list()) {
//                        final Integer sectionId = sectionInfo.getSection_id();
//                        final String sectionName = sectionInfo.getSection_name();
//                        final Integer sectionRankNum = sectionInfo.getRank_num();
//                        for (GroupInfo groupInfo : sectionInfo.getGroup_list()) {
//                            final Integer groupId = groupInfo.getGroup_id();
//                            final String groupName = groupInfo.getGroup_name();
//                            for (ContentInfo contentInfo : groupInfo.getContent_list()) {
//                                final Integer contentId = contentInfo.getContent_id();
//                                if (existContentIds.contains(contentId)) {
//                                    continue;
//                                }
//                                final String contentTitle = contentInfo.getContent_title();
//                                for (Content content : contentInfo.getContent()) {
//                                    final String callbackKey = StringUtils.defaultIfBlank(content.getCallback_key(), StringUtils.EMPTY);
//                                    final String materialUrl = StringUtils.defaultIfBlank(content.getUrl(), StringUtils.EMPTY);
//                                    final String materialName = StringUtils.defaultIfBlank(content.getName(), StringUtils.EMPTY);
//                                    final String videoUrl = Utils.getPlayUrlFromDetail(callbackKey);
//
//                                    final String sql = String.format("insert into craw_video_info(course_id, course_name, chapter_id, chapter_name, chapter_rank_num, section_id, section_name, section_rank_num, group_id, group_name, content_id, content_name, callback_key, material_url, material_name, video_url) values(%d, '%s', %d, '%s', %d, %d, '%s', %d, %d, '%s', %d, '%s', '%s', '%s', '%s', '%s')",
//                                            courseId, courseName, chapterId, chapterName, chapterRankNum, sectionId, sectionName, sectionRankNum, groupId, groupName, contentId, contentTitle, callbackKey, materialUrl, materialName, videoUrl);
//                                    System.out.println(sql);
//                                    try {
//                                        statement.execute(sql);
//                                        existContentIds.add(contentId);
//                                    } catch (MySQLIntegrityConstraintViolationException e) {
//                                        System.out.println(e.getMessage());
//                                    }
//                                }
//                            }
//                        }
//                    }
//                }
//            }
//        }
//    }
}
