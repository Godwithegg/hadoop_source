package com.danhuang.mr.partitionersort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

//按手机号进行分区排序 样本数input8
public class FlowCountSortDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"./src/main/resources/input8","./src/main/resources/output8"};
        Configuration conf = new Configuration();
        //1.获取job对象
        Job job = Job.getInstance(conf);

        //2.设置jar的路径
        job.setJarByClass(FlowCountSortDriver.class);

        //3.关联mapper和reducer
        job.setMapperClass(FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSortReducer.class);

        //关联分区
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);

        //4.设置mapper输出的key和value类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //5.设置最终输出的key和value类型
        job.setOutputValueClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //6.设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //7.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
