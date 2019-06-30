package com.danhuang.mr.cachejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class DistributedCacheDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        args = new String[]{"./src/main/resources/input13","./src/main/resources/output13"};
        Configuration conf = new Configuration();
        //1.获取job对象
        Job job = Job.getInstance(conf);

        //2.设置jar的路径
        job.setJarByClass(DistributedCacheDriver.class);

        //3.关联mapper和reducer
        job.setMapperClass(DistributedCacheMapper.class);

        //4.设置最终输出的key和value类型
        job.setOutputValueClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //5.设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //6.加载缓存数据,加到了整个集群上
        job.addCacheFile(new URI("./src/main/resources/cache13/pd.txt"));

        //7.Map端的join逻辑不需要Reduce阶段，设置reduceTask数量为0
        job.setNumReduceTasks(0);

        //8.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
