package com.yw.hadoop.mr.p04_partitioner;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author yangwei
 */
public class CustomPartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String phoneNum = text.toString();
        if (StringUtils.isNotBlank(phoneNum)) {
            if (StringUtils.startsWith(phoneNum, "135")) {
                return 0;
            } else if (StringUtils.startsWith(phoneNum, "136")) {
                return 1;
            } else if (StringUtils.startsWith(phoneNum, "137")) {
                return 2;
            } else if (StringUtils.startsWith(phoneNum, "138")) {
                return 3;
            } else if (StringUtils.startsWith(phoneNum, "139")) {
                return 4;
            } else {
                return 5;
            }
        } else {
            return 5;
        }
    }
}
