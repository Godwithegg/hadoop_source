package com.danhuang.mr.tablejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text,Text,TableBean> {

    String name;
    TableBean v = new TableBean();
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取文件的名称
        FileSplit inputSplit = (FileSplit) context.getInputSplit();

        name = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.获取一行
        String line = value.toString();

        if(name.startsWith("order")){//订单表
            String[] fields = line.split("\t");
            //封装key和value
            v.setId(fields[0]);
            v.setPid(fields[1]);
            v.setAmount(Integer.parseInt(fields[2]));
            v.setPname("");
            v.setFlag("order");
            k.set(fields[1]);
        }else {//产品表
            String[] fields = line.split("\t");
            //封装key和value
            v.setId("");
            v.setPid(fields[0]);
            v.setAmount(0);
            v.setPname(fields[1]);
            v.setFlag("pd");
            k.set(fields[0]);

        }
        context.write(k,v);
    }
}
