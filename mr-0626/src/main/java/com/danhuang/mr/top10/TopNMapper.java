package com.danhuang.mr.top10;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;

public class TopNMapper extends Mapper<LongWritable, Text,FlowBean,Text> {

    Text v = new Text();
    private TreeMap<FlowBean,Text> map = new TreeMap<FlowBean,Text>();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //因为treemap要加入的是对象，因此必须在方法里面new对象，而不是方法外边！！
        FlowBean bean = new FlowBean();

        String line = value.toString();
        String[] fields = line.split("\t");

        v.set(fields[0]);
        bean.setUpFlow(Long.parseLong(fields[1]));
        bean.setDownFLow(Long.parseLong(fields[2]));
        bean.setSumFlow(Long.parseLong(fields[3]));

        map.put(bean,v);
        if(map.size() > 10){
            map.remove(map.lastKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator<FlowBean> it = map.keySet().iterator();
        while(it.hasNext()){
            FlowBean k = it.next();
            context.write(k,map.get(k));
        }
    }
}
