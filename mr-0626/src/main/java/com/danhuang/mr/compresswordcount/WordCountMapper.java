package com.danhuang.mr.compresswordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * map 阶段
 * KEYIN 输入数据的key
 * VALUEIN 输入数据的value
 * KEYOUT 输出数据的key类型 danhuang,1 danbai,1
 * VALUEOUT 输出的数据value类型
 */

public class WordCountMapper extends Mapper<LongWritable, Text , Text , IntWritable> {//偏移量

    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //danhuang danhuang

        //1.获取一行
        String line = value.toString();

        //2.切割单词
        String[] words = line.split(" ");

        //3.循环写出
        for (String word : words) {
            //danhuang
            k.set(word);
            context.write(k , v);
        }
    }
}
