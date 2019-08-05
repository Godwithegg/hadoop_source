package com.danhuang.mr.jobseries;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TwoIndexReducer extends Reducer<Text,Text,Text,Text> {

    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //danhuang a.txt	3
        //danhuang b.txt	2
        //danhuang c.txt	2

        //danhuang	c.txt-->2	b.txt-->2	a.txt-->3

        //1.拼接
        StringBuffer sb = new StringBuffer();
        for(Text value : values){
            sb.append(value.toString().replace("\t","-->") + "\t");
        }

        //2.封装对象
        v.set(sb.toString());

        //3.写出
        context.write(key,v);
    }
}
