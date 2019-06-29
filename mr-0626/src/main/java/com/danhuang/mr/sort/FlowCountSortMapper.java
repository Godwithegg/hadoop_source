package com.danhuang.mr.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountSortMapper extends Mapper<LongWritable, Text,FlowBean,Text> {

    FlowBean k = new FlowBean();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.获取一行 13709384729 2401 120 2521
        String lines = value.toString();

        //2.切割
        String[] line = lines.split("\t");

        //3.封装对象
        k.setUpFlow(Long.parseLong(line[1]));
        k.setDownFlow(Long.parseLong(line[2]));
        k.setSumFlow(Long.parseLong(line[3]));
        v.set(line[0]);

        //4.写出
        context.write(k,v);
    }
}
