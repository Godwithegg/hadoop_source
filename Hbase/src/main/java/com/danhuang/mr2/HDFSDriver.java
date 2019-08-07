package com.danhuang.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HDFSDriver extends Configuration implements Tool {

    private Configuration conf = null;

    @Override
    public int run(String[] args) throws Exception {

        //获取job对象
        Job job = Job.getInstance(conf);

        //设置主类
        job.setJarByClass(HDFSDriver.class);

        //设置Mapper
        job.setMapperClass(HDFSMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Put.class);

        //设置Reducer
        TableMapReduceUtil.initTableReducerJob(
                "fruit2",
                HDFSReducer.class,
                job);

        //设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));

        //提交任务
        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = HBaseConfiguration.create();
        int i = ToolRunner.run(configuration, new HDFSDriver(), args);
        System.exit(i);
    }
}
