package com.yw.spark.example;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class Utils {
    private static final String AUTH = "Bearer pc:41c9019a2448e30a45747d57ff1a2c19";
    private static final String COOKIE = "Hm_lvt_9a1843872729d95c5d7acbea784c51b2=1648643859; zg_did=%7B%22did%22%3A%20%2217fdad4e317811-028ffafeeaa6f-1c3d645d-1aeaa0-17fdad4e318993%22%7D; zg_7c9bcf6917804ce5ad8448db3cbe3fb3=%7B%22sid%22%3A%201648643859.227%2C%22updated%22%3A%201648643859.23%2C%22info%22%3A%201648643859229%7D; gr_user_id=2d3b433a-79f6-473b-a7c5-6468281bb97d; kd_user_id=f0da3df4-af2e-4e5d-b812-00e8541a37f6; figui=6MQIfb68XPco83A3; Hm_lvt_156e88c022bf41570bf96e74d090ced7=1657358325,1659401620; deviceId=6701b2a2f00f89e6e1ad41ee60724fc3; 99f53b614ce96c83_gr_session_id=295e365c-2851-4b15-9fea-92cd27963a86; 99f53b614ce96c83_gr_session_id_295e365c-2851-4b15-9fea-92cd27963a86=true; ssoToken=9b078345d720ab9e028d2d7f7efaccd9; passportUid=3038866; access-edu_online=41c9019a2448e30a45747d57ff1a2c19; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%223038866%22%2C%22first_id%22%3A%2217fdad4f691de9-023f8fa07b9c44e-1c3d645d-1764000-17fdad4f6921025%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTdmZGFkNGY2OTFkZTktMDIzZjhmYTA3YjljNDRlLTFjM2Q2NDVkLTE3NjQwMDAtMTdmZGFkNGY2OTIxMDI1IiwiJGlkZW50aXR5X2xvZ2luX2lkIjoiMzAzODg2NiJ9%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%24identity_login_id%22%2C%22value%22%3A%223038866%22%7D%2C%22%24device_id%22%3A%2217fdad4f691de9-023f8fa07b9c44e-1c3d645d-1764000-17fdad4f6921025%22%7D; kkb_edu_session=eyJpdiI6InNHN3BIcjNaam1qa1wvempWQkdUK2l3PT0iLCJ2YWx1ZSI6IjgwdkhFUVdOZGYyazJkRjFQdmlza3U3Z1JwNTUrRVViXC9qZlpiUlNsV0E1TzFFTWpNamNHUlpQSzlOdXVuTTByIiwibWFjIjoiMTk3OWFlYmZhNTI4MDc4MTQ3NjA5ZmYzOGIxNTg1YjE1N2FlM2ZiZWUxZDU0ZjEyYTE1YmMwNTE4NjVlMTA4NCJ9";

//    // ZYL
//    private static final String AUTH = "Bearer pc:10702725644ce1c12c81d8e5c5fe9b9d";
//    private static final String COOKIE = "Hm_lvt_9a1843872729d95c5d7acbea784c51b2=1648643859; zg_did=%7B%22did%22%3A%20%2217fdad4e317811-028ffafeeaa6f-1c3d645d-1aeaa0-17fdad4e318993%22%7D; zg_7c9bcf6917804ce5ad8448db3cbe3fb3=%7B%22sid%22%3A%201648643859.227%2C%22updated%22%3A%201648643859.23%2C%22info%22%3A%201648643859229%7D; gr_user_id=2d3b433a-79f6-473b-a7c5-6468281bb97d; kd_user_id=f0da3df4-af2e-4e5d-b812-00e8541a37f6; figui=6MQIfb68XPco83A3; Hm_lvt_156e88c022bf41570bf96e74d090ced7=1657358325,1659401620; deviceId=6701b2a2f00f89e6e1ad41ee60724fc3; ssoToken=1a9928707535bd1f3f5c2ed4e239f690; passportUid=5321848; access-edu_online=10702725644ce1c12c81d8e5c5fe9b9d; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%225321848%22%2C%22first_id%22%3A%2217fdad4f691de9-023f8fa07b9c44e-1c3d645d-1764000-17fdad4f6921025%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTdmZGFkNGY2OTFkZTktMDIzZjhmYTA3YjljNDRlLTFjM2Q2NDVkLTE3NjQwMDAtMTdmZGFkNGY2OTIxMDI1IiwiJGlkZW50aXR5X2xvZ2luX2lkIjoiNTMyMTg0OCJ9%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%24identity_login_id%22%2C%22value%22%3A%225321848%22%7D%2C%22%24device_id%22%3A%2217fdad4f691de9-023f8fa07b9c44e-1c3d645d-1764000-17fdad4f6921025%22%7D; 99f53b614ce96c83_gr_session_id=4e67069f-8ba0-43db-8a20-293a7c6dd946; 99f53b614ce96c83_gr_session_id_4e67069f-8ba0-43db-8a20-293a7c6dd946=true; kkb_edu_session=eyJpdiI6Im1mTTFkQU9URmJQbVlHY1RrUlU4b3c9PSIsInZhbHVlIjoiY3VIRDZoU3VLWnc3MWxEUGdsWVUzSjlWeE9DcEIxM1wvXC9cL1wvbk1QUHo5MFhcLzQxdjM2b1wvTFwvY21xb3l0TjZZQjkiLCJtYWMiOiIzODc0ZDA5Mzk0MTY2NTQ3ODcxYTlmZjdlMTliMzRhNjU2MTVlZjViZjI0NzcxMjgyNThkMzhmMWI0YjBlNDA1In0%3D";

    public static void main(String[] args) {

//        for (Map.Entry<Integer, String> entry : getCourseList().entrySet()) {
//            final Integer courseId = entry.getKey();
//            final String courseName = entry.getValue();
//
//            final Map<Integer, String> courseInfo = Utils.getChapterList(courseId);
//            courseInfo.forEach((k, v) -> {
//                final String json = Utils.getDownloadJson(courseId, k);
//
//                System.out.println(json);
////            download(PATH + courseName + "/", json);
//            });
//        }

////        Integer courseId = 250047; // 新一代云原生MQ架构Pulsar实战
//        Integer courseId = 215859; // 大数据高级开发工程师课程
//        final Map<Integer, String> courseInfo = Utils.getChapterList(courseId);
//        courseInfo.forEach((k, v) -> {
//            final String json = Utils.getDownloadJson(courseId, k);
//
//            System.out.println(json);
////            download(PATH + v + "/", json);
//        });

        System.out.println(Utils.getPlayUrlFromDetail("media-853926875693056"));
    }

    public static Map<Integer, String> getCourseList() {
        Map<String, String> map = new HashMap<>();
        map.put("authorization", AUTH);
        map.put("cookie", COOKIE);
        final String json = getRequest("https://weblearn.kaikeba.com/student/opt/course/list?type=0&option=2", map);
        Map<Integer, String> result = new TreeMap<>(Comparator.naturalOrder());

        if (JSONObject.parseObject(json).getInteger("code") == 1) {
            final JSONArray data = JSONObject.parseObject(json).getJSONArray("data");
            if (data.size() > 0) {
                for (Object o : data) {
                    JSONObject course = (JSONObject) o;
                    final Integer courseId = course.getInteger("course_id");
                    final String courseName = course.getString("course_name");
                    System.out.println("课程ID 课程名称: " + courseId + " " + courseName);
                    result.put(courseId, courseName);
                }
            }
        }
        return result;
    }

    public static String getDownloadJson(Integer courseId, Integer chapterId) {
        Map<String, String> map = new HashMap<>();
        map.put("authorization", AUTH);
        map.put("cookie", COOKIE);
        return getRequest("https://weblearn.kaikeba.com/student/chapterinfo?course_id=" + courseId + "&chapter_id=" + chapterId + "&__timestamp=" + System.currentTimeMillis(), map);
    }

    public static Map<Integer, String> getChapterList(Integer courseId) {
        Map<String, String> map = new HashMap<>();
        map.put("authorization", AUTH);
        map.put("cookie", COOKIE);
        final String json = getRequest("https://weblearn.kaikeba.com/student/courseinfo?course_id=" + courseId + "&__timestamp=" + System.currentTimeMillis(), map);
        Map<Integer, String> result = new TreeMap<>(Comparator.naturalOrder());

        if (JSONObject.parseObject(json).getInteger("code") == 1) {
            final JSONObject data = JSONObject.parseObject(json).getJSONObject("data");
            System.out.println("课程ID 课程名称: " + courseId + " " + data.getString("course_name"));
            final JSONArray chapterList = data.getJSONArray("chapter_list");

            if (chapterList.size() > 0) {
                for (Object o : chapterList) {
                    JSONObject chapter = (JSONObject) o;
                    String chapterName = chapter.getString("rank_num") + "_" + revert(chapter.getString("chapter_name"));
                    Integer chapterId = chapter.getInteger("chapter_id");
                    result.put(chapterId, chapterName);
                }
            }
        }
        else {
            System.out.println(JSONObject.parseObject(json).getString("msg"));
        }

        return result;
    }

    private static String getAccessToken() {
        // {"code":1,"data":{"access_token":"61ff856282f946f5966553258e395edc"},"msg":""}
        /**
         * {
         *     "code": 1,
         *     "data": {
         *         "access_token": "61ff856282f946f5966553258e395edc"
         *     },
         *     "msg": ""
         * }
         */
        String json = getRequest("https://weblearn.kaikeba.com/get/bsy_video/access_token");
        JSONObject jsonObject = JSONObject.parseObject(json);
        String result = jsonObject.getJSONObject("data").getString("access_token");
//        System.out.println("access_token = " + result);
        return result;
    }

    public static String getPlayUrlFromDetail(String mediaId) {
        // {"code":1,"msg":"请求成功","data":{"mediaId":"media-852667894562816","tenantId":"2027176802","spaceName":"space-1","status":3,"title":"抱石云点播视频","mediaType":0,"mediaMetaInfo":{"videoGroup":[{"videoId":"video-852667905474560","extension":".m3u8","resolution":"超清","presetName":"lud","bucketName":"bsy-vod-output","objectName":"resource/media-852667894562816/lud/76a6b0b639f0455f9422637ff838c4a2.m3u8","isEncryption":true,"noAuth":true,"playURL":"https://v.baoshiyun.com/resource/media-852667894562816/lud/76a6b0b639f0455f9422637ff838c4a2.m3u8?MtsHlsUriToken=b75605f58629498ba743590e1d82c7dd8b58f589e2684616b8deefe4dc0b36ae","size":"27807832","duration":"500.613112"},{"videoId":"video-852667905540097","extension":".m3u8","resolution":"高清","presetName":"lhd","bucketName":"bsy-vod-output","objectName":"resource/media-852667894562816/lhd/d33ee30bbe8a45cf8ec7e50e2babbb06.m3u8","isEncryption":true,"noAuth":true,"playURL":"https://v.baoshiyun.com/resource/media-852667894562816/lhd/d33ee30bbe8a45cf8ec7e50e2babbb06.m3u8?MtsHlsUriToken=cbf3ca22623d44f092a9e3634e72c30481644fa662e8467eadb768e33f36b7fb","size":"18506344","duration":"500.613112"},{"videoId":"video-852667905540096","extension":".m3u8","resolution":"标清","presetName":"lsd","bucketName":"bsy-vod-output","objectName":"resource/media-852667894562816/lsd/e9c8a0a5ffa441e8a23271a8af612a42.m3u8","isEncryption":true,"noAuth":true,"playURL":"https://v.baoshiyun.com/resource/media-852667894562816/lsd/e9c8a0a5ffa441e8a23271a8af612a42.m3u8?MtsHlsUriToken=0df296b5612d4e3498c203cf1dbbcbcdf6ced4597af24741900cde8ff036df21","size":"14873432","duration":"500.613112"}],"imageGroup":[{"imageId":"img-852667906097152","type":"default","bucketName":"bsy-vod-output","objectName":"resource/media-852667894562816/18c76001c9994df8bef4b0bf232a4980_00001.jpg","coverImage":false,"showURL":"https://v.baoshiyun.com/resource/media-852667894562816/18c76001c9994df8bef4b0bf232a4980_00001.jpg"}]},"createTime":"2021-07-15 15:42:23"}}
        /**
         * {
         *     "code": 1,
         *     "msg": "请求成功",
         *     "data": {
         *         "mediaId": "media-852667894562816",
         *         "tenantId": "2027176802",
         *         "spaceName": "space-1",
         *         "status": 3,
         *         "title": "抱石云点播视频",
         *         "mediaType": 0,
         *         "mediaMetaInfo": {
         *             "videoGroup": [
         *                 {
         *                     "videoId": "video-852667905474560",
         *                     "extension": ".m3u8",
         *                     "resolution": "超清",
         *                     "presetName": "lud",
         *                     "bucketName": "bsy-vod-output",
         *                     "objectName": "resource/media-852667894562816/lud/76a6b0b639f0455f9422637ff838c4a2.m3u8",
         *                     "isEncryption": true,
         *                     "noAuth": true,
         *                     "playURL": "https://v.baoshiyun.com/resource/media-852667894562816/lud/76a6b0b639f0455f9422637ff838c4a2.m3u8?MtsHlsUriToken=b75605f58629498ba743590e1d82c7dd8b58f589e2684616b8deefe4dc0b36ae",
         *                     "size": "27807832",
         *                     "duration": "500.613112"
         *                 },
         *                 {
         *                     "videoId": "video-852667905540097",
         *                     "extension": ".m3u8",
         *                     "resolution": "高清",
         *                     "presetName": "lhd",
         *                     "bucketName": "bsy-vod-output",
         *                     "objectName": "resource/media-852667894562816/lhd/d33ee30bbe8a45cf8ec7e50e2babbb06.m3u8",
         *                     "isEncryption": true,
         *                     "noAuth": true,
         *                     "playURL": "https://v.baoshiyun.com/resource/media-852667894562816/lhd/d33ee30bbe8a45cf8ec7e50e2babbb06.m3u8?MtsHlsUriToken=cbf3ca22623d44f092a9e3634e72c30481644fa662e8467eadb768e33f36b7fb",
         *                     "size": "18506344",
         *                     "duration": "500.613112"
         *                 },
         *                 {
         *                     "videoId": "video-852667905540096",
         *                     "extension": ".m3u8",
         *                     "resolution": "标清",
         *                     "presetName": "lsd",
         *                     "bucketName": "bsy-vod-output",
         *                     "objectName": "resource/media-852667894562816/lsd/e9c8a0a5ffa441e8a23271a8af612a42.m3u8",
         *                     "isEncryption": true,
         *                     "noAuth": true,
         *                     "playURL": "https://v.baoshiyun.com/resource/media-852667894562816/lsd/e9c8a0a5ffa441e8a23271a8af612a42.m3u8?MtsHlsUriToken=0df296b5612d4e3498c203cf1dbbcbcdf6ced4597af24741900cde8ff036df21",
         *                     "size": "14873432",
         *                     "duration": "500.613112"
         *                 }
         *             ],
         *             "imageGroup": [
         *                 {
         *                     "imageId": "img-852667906097152",
         *                     "type": "default",
         *                     "bucketName": "bsy-vod-output",
         *                     "objectName": "resource/media-852667894562816/18c76001c9994df8bef4b0bf232a4980_00001.jpg",
         *                     "coverImage": false,
         *                     "showURL": "https://v.baoshiyun.com/resource/media-852667894562816/18c76001c9994df8bef4b0bf232a4980_00001.jpg"
         *                 }
         *             ]
         *         },
         *         "createTime": "2021-07-15 15:42:23"
         *     }
         * }
         */
        String accessToken = getAccessToken();
        String json = getRequest("https://api-vod.baoshiyun.com/vod/v1/platform/media/detail?mediaId=" + mediaId + "&accessToken=" + accessToken);
        JSONObject video = JSONObject.parseObject(json).getJSONObject("data").getJSONObject("mediaMetaInfo")
                .getJSONArray("videoGroup").getJSONObject(0);

        return video.getString("playURL");
    }

    public static Map<String, String> getTitleAndKey(String json) {
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONArray sectionList = jsonObject.getJSONObject("data").getJSONArray("section_list");
        Map<String, String> result = new TreeMap<>(Comparator.naturalOrder());

        if (sectionList.size() > 1) {
            for (Object o : sectionList) {
                JSONObject section = (JSONObject) o;
                String sectionName = section.getString("section_name");
                JSONArray contentList = section.getJSONArray("group_list")
                        .getJSONObject(0).getJSONArray("content_list");
                parse(contentList, result, sectionName);
            }
        } else {
            JSONArray contentList = sectionList
                    .getJSONObject(0)
                    .getJSONArray("group_list")
                    .getJSONObject(0).getJSONArray("content_list");

            parse(contentList, result, null);
        }
//        result.forEach((k, v) -> System.out.println(k + ": " + v));
        return result;
    }

    private static void parse(JSONArray contentList, Map<String, String> result, String sectionName) {
        if (contentList == null || contentList.size() == 0) {
            return;
        }
        for (Object o : contentList) {
            JSONObject content = (JSONObject) o;
            JSONArray content2 = content.getJSONArray("content");
            if (content2 == null || content2.size() == 0) {
                continue;
            }
            String key = content2.getJSONObject(0).getString("callback_key");
            if (StringUtils.isBlank(key)) {
                continue;
            }
            String title = revert(content.getString("content_title"));
            if (StringUtils.isNotBlank(sectionName)) {
                title = sectionName.replace(" ", "").replace("/", "_") + "/" + title;
            }
            result.put(title, key);
        }
    }

    private static String revert(String s) {
        return s.replace("/", "_")
                .replace("\\.\\ ", "_")
                .replace("\\.", "_")
                .replace("、", "_")
                .replace("&", "_")
                .replace("+", "_")
                .replace(" ", "_")
                .replace("(", "\\(")
                .replace("（", "\\（")
                .replace(")", "\\)")
                .replace("）", "\\）");
    }

    private static String getRequest(String url) {
        return getRequest(url, null);
    }

    private static String getRequest(String url, Map<String, String> map) {
        try {
            URL u = new URL(url);
            URLConnection conn = u.openConnection();
            if (map != null) {
                map.forEach(conn::setRequestProperty);
            }
            InputStream is = conn.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            StringBuffer result = new StringBuffer();
            String line;
            while ((line = br.readLine()) != null) {
                result.append(line);
            }
            return result.toString();
        } catch (Exception e) {
            return null;
        }
    }

    public static void exec(String command) {
        System.out.println(command);
        String[] cmd = new String[]{"/bin/sh", "-c", command};
        try {
            Process ps = Runtime.getRuntime().exec(cmd);
            BufferedReader br = new BufferedReader(new InputStreamReader(ps.getErrorStream()));
            String line;
            while ((line = br.readLine()) != null) {
                if (StringUtils.contains(line, "video:") && StringUtils.contains(line, "audio:")) {
                    System.out.println(line);
                    System.out.println("==>> done!!!! " + command);
                }
            }
            ps.waitFor();
            ps.destroy();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}