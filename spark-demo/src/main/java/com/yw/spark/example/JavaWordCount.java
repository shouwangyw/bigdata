package com.yw.spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author yangwei
 */
public class JavaWordCount {
    public static void main(String[] args) {
        // 1. 创建 SparkConf 对象
        SparkConf sparkConf = new SparkConf()
                .setAppName(JavaWordCount.class.getSimpleName())
                .setMaster("local[*]");

        // 2. 构建 JavaSparkContext 对象
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // 3. 读取数据文件
        JavaRDD<String> lines = jsc.textFile(ClassLoader.getSystemResource("word.csv").getPath());

        // 4. 切分每一行获取所有的单词
        // 将每行数据，生成一个由单词组成的数组；然后每行组成的数组进行 flat，生成最终的所有的单词组成的数组 ==>> RDD
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(",")).iterator());

        // 5. 将单词变成(word, 1)
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(word -> new Tuple2<>(word, 1));

        // 6. 将 wordAndOne 按照 key 分组，每组 value 进行聚合
        JavaPairRDD<String, Integer> wordAndTotal = wordAndOne.reduceByKey(Integer::sum);

        // 7. 结果，按照每组的个数进行排序
        /**
         * (hadoop, 2)                  (hive, 1)
         * (spark, 3)       ==>>        (hadoop, 2)
         * (hive, 1)                    (spark, 3)
         *
         * ① 为了实现按照数值进行排序的功能，首先将每个kv对的kv进行调换位置 (hadoop, 2) 变成了(2, hadoop)
         * ② 然后按照k进行排序（这是的key是数值） (2, hadoop)
         * ③ 排完顺序后，再将kv调换顺序，(word, totalCount)的形式
         */
        JavaPairRDD<Integer, String> totalAndWord = wordAndTotal.mapToPair(v -> new Tuple2<>(v._2, v._1));
        JavaPairRDD<Integer, String> sortedTotalAndWord = totalAndWord.sortByKey(true);
        JavaPairRDD<String, Integer> sortedResult = sortedTotalAndWord.mapToPair(v -> new Tuple2<>(v._2, v._1));

        List<Tuple2<String, Integer>> result = sortedResult.collect();
        for (Tuple2<String, Integer> tuple2 : result) {
            System.out.println("单词：" + tuple2._1 + ", 个数：" + tuple2._2);
        }

        jsc.stop();
    }
}
