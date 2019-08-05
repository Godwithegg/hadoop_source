package com.danhuang.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class CustomerConsumer {

    public static void main(String[] args) {

        //配置信息
        Properties props = new Properties();
        //Kafka集群
        props.put("bootstrap.servers", "hadoop102:9092");
        //消费者组id
        props.put("group.id", "test");
        //设置自动提交offset
        props.put("enable.auto.commit", "true");
        //提交延时
        props.put("auto.commit.interval.ms", "1000");
        //KV的反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //制定Topic
        consumer.subscribe(Arrays.asList("second","first","third"));

        while(true){
            //获取数据
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.out.println(record.topic()+"--"+record.partition()+"--"+record.value());
            }
        }
    }
}
