package com.yw.recommend.service.impl;

import com.yw.recommend.service.RcmdService;
import redis.clients.jedis.Jedis;

import java.util.*;

public class RcmdServiceImpl implements RcmdService {
    @Override
    public List<String> getRcmdList(String uid) {
        // 获得数据库连接
        Jedis jedis = new Jedis("node4", 6379);
        jedis.select(2);
        // 从用户历史下载表来获取最近下载
        String downloadListString = jedis.hget("rcmd_user_history", uid);
        String[] downloadList = downloadListString.split(",");
        System.out.println(uid + " downloadList:" + Arrays.toString(downloadList));

        // 获取所有应用ID列表
        Set<String> appList = jedis.hkeys("rcmd_item_list");

        // 存储总的特征分值
        Map<String, Double> scores = new HashMap<>();

        // 分别计算所有应用的总权重  appList商城中所有的app
        for (String appId : appList) {
            // 计算关联权重
            double relativeFeatureScore = this.getRelativeFeatureScore(appId, downloadList, jedis);
            updateScoresMap(scores, appId, relativeFeatureScore);
            // 计算基本特征权重之和
            double basicFeatureScore = this.getBasicFeatureScore(appId, jedis);
            updateScoresMap(scores, appId, basicFeatureScore);
        }

        // 这里将map.entrySet()转换成list
        List<Map.Entry<String, Double>> list = new ArrayList<>(scores.entrySet());
        // 然后通过比较器来实现排序
        // 升序排序
        list.sort((o1, o2) -> -o1.getValue().compareTo(o2.getValue()));
        // 打印分值
        for (Map.Entry<String, Double> mapping : list) {
            System.out.println(mapping.getKey() + ":" + mapping.getValue());
        }
        // 取前10个appID返回
        List<String> result = new ArrayList<>();
        int count = 0;
        for (Map.Entry<String, Double> mapping : list) {
            count++;
            result.add(mapping.getKey());
            if (count == 10) {
                break;
            }
        }
        jedis.close();
        return result;
    }

    private void updateScoresMap(Map<String, Double> scores, String appName, double score) {
        scores.merge(appName, score, Double::sum);
    }

    // 获取商品关联特征权重
    private double getRelativeFeatureScore(String appId, String[] downloadList, Jedis jedis) {
        double score = 0.0;
        // 得到所有的关联特征所对应的权重之和
        for (String downloadAppId : downloadList) {
//        Item.id*Item.id@70*193
            // 构成关联特征
            String feature = "Item.id*Item.id@" + appId + "*" + downloadAppId;
            String rcmdFeaturesScore = jedis.hget("rcmd_features_score", feature);
            if (rcmdFeaturesScore != null) {
                score += Double.parseDouble(rcmdFeaturesScore);
            }
//            String featurex = "Item.id*Item.id@" + downloadAppId + "*" + appId;
//            String rcmd_features_scorex = jedis.hget("rcmd_features_score", featurex);
//            if(rcmd_features_scorex!=null) {
//                score += Double.valueOf(rcmd_features_scorex);
//            }
        }
        return score;
    }

    private double getBasicFeatureScore(String appId, Jedis jedis) {
        // 存储基本特征分值
        double basicScore = 0.0;

        // 从商品词表取基本特征
        /*
            Item.id@146 软件ID
            Item.name@183   名字
            Item.author@zhouming    作者
            Item.sversion@1.3.2 版本号
            Item.ischarge@1 是否收费
            Item.dgner@husheng  设计者
            Item.font@Consolos  字体
            Item.icount@4   图片数量
            Item.icount_dscrt@4
            Item.stars@5    星级
            Item.price  价格
            Item.fsize@6  文件大小
            Item.fsize_dscrt@6
            Item.comNum@0   评论数量
            Item.comNum_dscrt@0
            Item.screen@FHD 屏幕类型
            Item.downNum@200  下载数
            Item.downNum_dscrt@200
         */
        String[] basicFeatureNames = {"Item.id", "Item.name", "Item.author", "Item.sversion", "Item.ischarge"
                , "Item.dgner", "Item.font", "Item.icount", "Item.icount_dscrt", "Item.stars", "Item.price"
                , "Item.fsize", "Item.fsize_dscrt", "Item.comNum", "Item.comNum_dscrt", "Item.screen", "Item.downNum"
                , "Item.downNum_dscrt"};
        String rcmdItemList = jedis.hget("rcmd_item_list", appId);
        String[] basicFeatures = rcmdItemList.split("\t");
        // 累加的app基本特征所对应的权重之和
        for (int i = 0; i < basicFeatureNames.length; i++) {
            String rcmdFeaturesScore = jedis.hget("rcmd_features_score", basicFeatureNames[i] + "@" + basicFeatures[i]);
            if (rcmdFeaturesScore != null) {
                basicScore += Double.parseDouble(rcmdFeaturesScore);
            }
        }
        return basicScore;
    }
}
