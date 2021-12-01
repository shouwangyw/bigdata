package com.yw.hadoop.mr.p08_grouping;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author yangwei
 */
public class OrderBean implements WritableComparable<OrderBean> {
    private String orderId;
    private Double price;
    /**
     * 自定义比较规则
     */
    @Override
    public int compareTo(OrderBean o) {
        int result = this.orderId.compareTo(o.orderId);
        if (result == 0) {
            result = -this.price.compareTo(o.price);
        }
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.price = in.readDouble();
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return orderId + "\t" + price;
    }
}
