package com.danhuang.mr.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;


public class FRecordWriter extends RecordWriter<Text,NullWritable> {

    FSDataOutputStream fosdanhuang;
    FSDataOutputStream fosother;

    public FRecordWriter(TaskAttemptContext job) {

        try {
            //1.获取文件系统
            FileSystem fs = FileSystem.get(job.getConfiguration());

            //2.创建输出到danhuang.log的输出流
            fosdanhuang = fs.create(new Path("./src/main/resources/out11/danhuang.log"));

            //3.创建输出到other.log的输出流
            fosother = fs.create(new Path("./src/main/resources/out11/other.log"));

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        //判断key中是否有danhuang，如果有写出到danhuang.log，如果没有写出到other.log
        if(key.toString().contains("danhuang")){
            //danhuang的输出流
            fosdanhuang.write(key.toString().getBytes());
        }else {
            fosother.write(key.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(fosdanhuang);
        IOUtils.closeStream(fosother);
    }
}
