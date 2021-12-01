package com.yw.hadoop.mr.p08_grouping;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author yangwei
 */
public class GroupPartitioner extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numPartitions) {
        // 将订单号相同的分在一个区
        return orderBean.getOrderId().hashCode() % numPartitions;
    }
}
