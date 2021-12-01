package com.yw.hadoop.mr.p12_reduce_join;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yangwei
 */
public class ReduceJoinReducer extends Reducer<Text, Text, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 订单数据有多条
        List<String> orders = new ArrayList<>();
        // 保存商品信息
        String product = "";

        for (Text value : values) {
            if (value.toString().startsWith("p")) {
                product = value.toString();
            } else {
                orders.add(value.toString());
            }
        }
        for (String order : orders) {
            context.write(new Text(order + "\t" + product), NullWritable.get());
        }
    }
}
