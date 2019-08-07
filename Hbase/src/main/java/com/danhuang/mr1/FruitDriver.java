package com.danhuang.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FruitDriver extends Configuration implements Tool {

    private Configuration conf = new Configuration();

    @Override
    public int run(String[] strings) throws Exception {

        //获取任务对象
        Job job = Job.getInstance(conf);

        //指定Driver类
        job.setJarByClass(FruitDriver.class);

        //指定Mapper类
        TableMapReduceUtil.initTableMapperJob(
                "fruit",
                new Scan(),
                FruitMapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job);

        //执行Reducer
        TableMapReduceUtil.initTableReducerJob(
                "fruit_mr",
                FruitReducer.class,
                job);

        //提交
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
        int i = ToolRunner.run(configuration, new FruitDriver(), args);
    }
}
