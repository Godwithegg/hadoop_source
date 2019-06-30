package com.danhuang.mr.grouporder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


//按照id号进行升序排序，价格降序排序，相同id只取价格最高的，只取前三位 样本input10
public class OrderDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"./src/main/resources/input10","./src/main/resources/output10"};
        Configuration conf = new Configuration();
        //1.获取job对象
        Job job = Job.getInstance(conf);

        //2.设置jar的路径
        job.setJarByClass(OrderDriver.class);

        //3.关联mapper和reducer
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        //4.设置mapper输出的key和value类型
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //5.设置最终输出的key和value类型
        job.setOutputValueClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        //8.设置reducer端的分组分组
        job.setGroupingComparatorClass(OrderGroupingComparator.class);

        //6.设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //7.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
