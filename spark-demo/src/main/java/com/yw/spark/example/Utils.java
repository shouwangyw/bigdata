package com.yw.spark.example;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Utils {
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
            String title = content.getString("content_title")
                    .replace("/", "_")
                    .replace("\\.\\ ", "_")
                    .replace("\\.", "_")
                    .replace("、", "_")
                    .replace("&", "_")
                    .replace(" ", "_")
                    .replace("(", "\\(")
                    .replace(")", "\\)");
            if (StringUtils.isNotBlank(sectionName)) {
                title = sectionName.replace(" ", "").replace("/", "_") + "/" + title;
            }
            result.put(title, key);
        }
    }

    private static String getRequest(String url) {
        try {
            URL u = new URL(url);
            URLConnection conn = u.openConnection();
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