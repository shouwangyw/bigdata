package com.yw.hadoop.mr.p13_map_join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author yangwei
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    /**
     * 用于保存商品表的数据，key: 商品id, value: 与key对应的表记录
     */
    private Map<String, String> productMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        productMap = new HashMap<>();
        // 获取到所有的缓存文件
        // 方式一：
        Configuration configuration = context.getConfiguration();
        URI[] cacheFiles = Job.getInstance(configuration).getCacheFiles();
//        // 方式二：
//        URI[] cacheFiles = DistributedCache.getCacheFiles(configuration);

        // 本例只有一个缓存文件放进了分布式缓存
        URI cacheFile = cacheFiles[0];

        FileSystem fileSystem = FileSystem.get(configuration);
        FSDataInputStream fsdis = fileSystem.open(new Path(cacheFile));

        try (BufferedReader br = new BufferedReader(new InputStreamReader(fsdis))){
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] slices = line.split(",");
                productMap.put(slices[0], line);
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] slices = value.toString().split(",");
        // 获取订单的商品id
        String pid = slices[2];
        // 获取商品表的数据
        String pdtsLine = productMap.get(pid);
        context.write(new Text(value.toString() + "\t" + pdtsLine), NullWritable.get());
    }
}
