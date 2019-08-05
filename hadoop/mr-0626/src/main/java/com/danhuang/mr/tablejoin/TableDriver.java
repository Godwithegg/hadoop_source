package com.danhuang.mr.tablejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

//join表连接字段 样本input12
public class TableDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"./src/main/resources/input12","./src/main/resources/output12"};
        Configuration conf = new Configuration();
        //1.获取job对象
        Job job = Job.getInstance(conf);

        //2.设置jar的路径
        job.setJarByClass(TableDriver.class);

        //3.关联mapper和reducer
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);

        //4.设置mapper输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        //5.设置最终输出的key和value类型
        job.setOutputValueClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        //6.设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //7.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
