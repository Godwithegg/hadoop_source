package com.danhuang.mr.phonepartitioner;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text key, FlowBean value, int numPartitions) {

        //key是手机号
        //value流量信息

        //分区号，一共是5个分区（136，137，138，139和其他）
        int parttion = 4;
        //1.获取手机号前三位
        String prePhoneNum = key.toString().substring(0,3);
        if("136".equals(prePhoneNum)){
            parttion = 0;
        }else if("137".equals(prePhoneNum)){
            parttion = 1;
        }else if("138".equals(prePhoneNum)){
            parttion = 2;
        }else if("139".equals(prePhoneNum)){
            parttion = 3;
        }
        return parttion;
    }
}
