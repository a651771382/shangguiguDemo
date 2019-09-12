package com.lym.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author:李雁敏
 * @create:2019-09-03 15:10
 */
public class CountInterceptor implements ProducerInterceptor<String, String> {
    int sucess;
    int error;

    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (metadata != null) {
            sucess++;
        } else {
            error++;
        }
    }

    public void close() {
        System.out.println("sucess:" + sucess + "\terror:" + error);
    }

    public void configure(Map<String, ?> configs) {

    }
}
