package com.danhuang.producer;

import kafka.Kafka;
import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.Properties;

public class CustomerProducer {

    public static void main(String[] args) {

        //配置信息
        Properties props = new Properties();
        //kafka集群
        props.put("bootstrap.servers", "hadoop102:9092");
        //应答级别
        props.put("acks", "all");
        //重试次数
        props.put("retries", 0);
        //批量大小
        props.put("batch.size", 16384);
        //提交延时
        props.put("linger.ms", 1);
        //缓存
        props.put("buffer.memory", 33554432);
        //KV的序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("partitioner.class","com.danhuang.producer.CustomerPartitioner");

        ArrayList<String> list = new ArrayList<>();
        list.add("com.danhuang.interceptor.TimeInterceptor");
        list.add("com.danhuang.interceptor.CountInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,list);

        //创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        //循环发送数据
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("second", String.valueOf(i))
                    , (metadata, exception) -> {
                        if(exception == null){
                            System.out.println(metadata.partition() + "--" + metadata.offset());
                        }else{
                            System.out.println("发送失败");
                        }
                    });
        }

        //关闭资源
        producer.close();
    }
}
