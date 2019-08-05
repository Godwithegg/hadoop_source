package com.danhuang.mr.logclean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

//数据清洗 过滤 样本input14
public class LogDriver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        args = new String[]{"./src/main/resources/input14","./src/main/resources/output14"};
        Configuration conf = new Configuration();

        //1.获取job对象
        Job job = Job.getInstance(conf);

        //2.设置jar存储路径
        job.setJarByClass(LogDriver.class);

        //3.关联mapper
        job.setMapperClass(LogMapper.class);

        //4.设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //5.设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //6.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
