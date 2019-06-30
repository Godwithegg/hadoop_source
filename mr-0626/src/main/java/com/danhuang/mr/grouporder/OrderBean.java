package com.danhuang.mr.grouporder;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    private int order_id;   //订单id
    private double price;   //订单价格

    public OrderBean(){

    }

    public OrderBean(int order_id, double price) {
        this.order_id = order_id;
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean bean) {

        //先按照id升序排序，如果相同，按照价格降序排序

        int result;
        if(order_id > bean.getOrder_id()){
            result = 1;
        }else if(order_id < bean.getOrder_id()){
            result = -1;
        }else {
            if(price > bean.getPrice()){
                result = -1;
            }else if(price < bean.getPrice()){
                result = 1;
            }else {
                result = 0;
            }
        }

        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(order_id);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        order_id = in.readInt();
        price = in.readDouble();
    }

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return order_id + "\t" + price;
    }
}
