package com.lym.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author:李雁敏
 * @create:2019-09-03 15:03
 */
public class TimeInterceptor implements ProducerInterceptor<String, String> {
    public void configure(Map<String, ?> configs) {

    }

    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        //取出数据
        String value = record.value();
        //创建一个新的ProducerRecord对象，并返回
        return new ProducerRecord<String, String>(record.topic(), record.partition(),
                record.key(), System.currentTimeMillis() + "," + record.value());
    }

    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    public void close() {

    }


}
