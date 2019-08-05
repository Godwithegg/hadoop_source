package com.danhuang.mr.partitionersort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<FlowBean, Text> {

    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {

        //按照手机号前三位分区
        int partitioner=4;
        String preNumber = text.toString().substring(0,3);

        if(preNumber.equals("136")){
            partitioner = 0;
        }else if(preNumber.equals("137")){
            partitioner = 1;
        }else if(preNumber.equals("138")){
            partitioner = 2;
        }else if(preNumber.equals("139")){
            partitioner = 3;
        }

        return partitioner;
    }
}
