package com.danhuang.mr.phonepartitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 根据手机的前三位划分省份 样本为input6
 */
public class FlowCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"./src/main/resources/input6","./src/main/resources/output6"};
        Configuration conf = new Configuration();
        //1.获取job对象
        Job job = Job.getInstance(conf);

        //2.设置jar的路径
        job.setJarByClass(FlowCountDriver.class);

        //3.关联mapper和reducer
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        //4.设置mapper输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5.设置最终输出的key和value类型
        job.setOutputValueClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //设置分区
        job.setPartitionerClass(ProvincePartitioner.class);
        //设置reducetask，如果设置为1则只有一个来处理map信息，最终分区仍然是1个文件，如果设置为2-4，分区的内容
        //不知道要由那个task执行，则会报io异常，如果设置为>5的数，那么多余的task会生成空文件
        job.setNumReduceTasks(5);

        //6.设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //7.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
