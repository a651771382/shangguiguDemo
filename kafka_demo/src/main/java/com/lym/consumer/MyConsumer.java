package com.lym.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author:李雁敏
 * @create:2019-09-03 11:11
 */
public class MyConsumer {
    public static void main(String[] args) {
        //创建消费者的配置信息
        Properties properties = new Properties();
        //给配置信息赋值
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "ceshi:9092");//kafka集群地址
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);//开启自动提交
//        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");//offset自动提交时长（毫秒）
        //kafka数据key、value的反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "bigdata");//消费者组
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");//重置消费者的offset

        //创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        //订阅主题
        consumer.subscribe(Arrays.asList("first"));
        while (true) {
            //获取数据
            ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
            //解析并打印ConsumerRecords
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.key() + "-----" + consumerRecord.topic() +
                        "--" + consumerRecord.partition() + "-----" + consumerRecord.offset());
            }
        }

    }
}
