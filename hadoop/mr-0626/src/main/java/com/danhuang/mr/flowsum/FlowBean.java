package com.danhuang.mr.flowsum;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {

    private long upFlow;    //上行流量
    private long downFLow;  //下行流量
    private long sumFlow;   //总流量

    //空参构造，为了后续反射用
    public FlowBean() {
        super();
    }

    public FlowBean(long upFlow, long downFLow) {
        super();
        this.upFlow = upFlow;
        this.downFLow = downFLow;
        sumFlow = upFlow + downFLow;
    }

    //序列化方法
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFLow);
        dataOutput.writeLong(sumFlow);
    }

    //反序列化方法
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //要求必须和序列化方法顺序一致
        upFlow = dataInput.readLong();
        downFLow = dataInput.readLong();
        sumFlow = dataInput.readLong();

    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFLow + "\t" + sumFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFLow() {
        return downFLow;
    }

    public void setDownFLow(long downFLow) {
        this.downFLow = downFLow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void set(long upFlow2,long downFlow2) {
        upFlow = upFlow2;
        downFLow = downFlow2;
        sumFlow = upFlow2 + downFlow2;
    }
}
