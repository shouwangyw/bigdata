package com.yw.hadoop.mr.p08_grouping;

import org.apache.hadoop.io.WritableComparator;

/**
 * @author yangwei
 */
public class MyGroup extends WritableComparator {
    public MyGroup() {
        // 分组类，要对OrderBean类型的key进行分组
        super(OrderBean.class, true);
    }

    @Override
    public int compare(Object a, Object b) {
        OrderBean a1 = (OrderBean) a;
        OrderBean b1 = (OrderBean) b;
        return a1.getOrderId().compareTo(b1.getOrderId());
    }
}
