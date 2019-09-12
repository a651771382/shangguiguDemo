package com.lym.producer;

import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author:李雁敏
 * @create:2019-09-03 09:12
 */
public class CallBackProducer {
    public static void main(String[] args) {
        //创建配置信息
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ceshi:9092");//kafka集群
        properties.put(ProducerConfig.ACKS_CONFIG, "-1");//ACK应答级别
        //数据key的序列化方式
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //数据value的序列化方式
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        //创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        List<String> list = new ArrayList<String>();
        list.add("a");
        list.add("b");
        list.add("c");

        //发送消息
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("first", 0, "atguigu",
                    "atguigu--" + i), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println(metadata.partition() + "-----***" + metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                }
            });
        }

        producer.close();
    }
}
