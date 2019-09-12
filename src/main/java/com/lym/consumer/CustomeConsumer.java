package com.lym.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author:李雁敏
 * @create:2019-08-27 16:59
 * 高级API
 */
public class CustomeConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "ceshi:9092"); //kafka集群
        props.put("group.id", "test"); //消费者组
        props.put("enable.auto.commit", "true"); //是否自动提交offset
        props.put("auto.commit.interval.ms", "1000"); //提交offset的延时
        //kafka key的反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //kafka value的反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        //指定topic
        consumer.subscribe(Arrays.asList("second", "first"));
        while (true) {
            ConsumerRecords<String, String> poll = consumer.poll(100);
            for (ConsumerRecord<String, String> record : poll) {
                System.out.println(record.topic() + "----" + record.partition() + "---" + record.offset());
            }
        }
    }
}
