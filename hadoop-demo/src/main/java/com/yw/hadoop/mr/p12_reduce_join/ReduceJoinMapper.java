package com.yw.hadoop.mr.p12_reduce_join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yangwei
 */
public class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /*
            现在我们读取了两个文件，如何确定当前处理的这一行数据是来自哪一个文件里面的?
            方式一：通过获取文件的切片，获得文件名
        // 获取我们输入的文件的切片
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        // 获取文件名
        String fileName = inputSplit.getPath().getName();
        if (StringUtils.equals(fileName, "order.txt")) {
            // 订单表数据
        } else {
            // 商品表数据
        }
         */

        // 方式二：因为t_product表，都是以p开头，所以可以作为判断的依据
        String[] slices = value.toString().split(",");
        if (value.toString().startsWith("p")) {
            // 样例数据: p0001,小米5,1000,2000
            context.write(new Text(slices[0]), value);
        } else {
            // order: 1001,20150710,p0001,2
            context.write(new Text(slices[2]), value);
        }
    }
}
