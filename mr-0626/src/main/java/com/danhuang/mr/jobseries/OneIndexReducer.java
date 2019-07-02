package com.danhuang.mr.jobseries;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OneIndexReducer extends Reducer<Text, IntWritable,Text,NullWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        String line = key.toString();
        String[] fields = line.split("--");
        int sum = 0;
        for(IntWritable value:values){
            sum += value.get();
        }
        k.set(fields[0] + "\t" +fields[1] + "-->" + sum);
        context.write(k, NullWritable.get());
    }
}
