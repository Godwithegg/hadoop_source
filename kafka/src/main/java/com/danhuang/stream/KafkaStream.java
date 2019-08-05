package com.danhuang.stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

public class KafkaStream {

    public static void main(String[] args) {

        //创建拓扑对象
        TopologyBuilder builder = new TopologyBuilder();
        //创建配置文件
        Properties properties = new Properties();
        properties.put("bootstrap.servers","hadoop102:9092");
        properties.put("application.id","kafkaStream");

        //构建拓扑结构
        builder.addSource("SOURCE","first")
                .addProcessor("PROCESSOR", () -> new LogProcessor(), "SOURCE")
                .addSink("SINK","second","PROCESSOR");

        KafkaStreams kafkaStreams = new KafkaStreams(builder, properties);

        kafkaStreams.start();
    }
}
