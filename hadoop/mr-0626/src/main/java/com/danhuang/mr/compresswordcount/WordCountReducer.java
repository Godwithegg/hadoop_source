package com.danhuang.mr.compresswordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//KEYIN，VLUEIN map阶段输出的key和value
public class WordCountReducer extends Reducer<Text , IntWritable , Text , IntWritable> {

    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //danhuang,1
        //danhuang,1

        int sum = 0;
        //1.累加求和
        for (IntWritable value : values){
            sum += value.get();
        }

        v.set(sum);
        //2.写出 danhuang,2
        context.write(key , v);
    }
}
