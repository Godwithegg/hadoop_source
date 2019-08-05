package com.danhuang.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CountInterceptor implements ProducerInterceptor<String,String> {

    private int successCount = 0;
    private int errorCOunt = 0;
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if(exception == null){
            successCount++;
        }else{
            errorCOunt++;
        }
    }

    @Override
    public void close() {
        System.out.println("发送成功:"+successCount+"条数据！！");
        System.out.println("发送失败:"+errorCOunt+"条数据！！");
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
