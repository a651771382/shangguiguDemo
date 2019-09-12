package com.lym.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author:李雁敏
 * @create:2019-08-27 15:27
 * 高级API
 */
public class CustomerProducer {
    public static void main(String[] args) {

        //配置信息
        Properties props = new Properties();
        props.put("bootstrap.servers", "ceshi:9092");//kafka集群
        props.put(ProducerConfig.ACKS_CONFIG, "-1"); //应答级别
        props.put("retries", 2); //重试次数
        props.put("batch.size", 16384); //批次的大小
        props.put("linger.ms", 1); //提交延时
        props.put("buffer.memory", 33554432); //生产者缓存
        //kafka数据key的序列化方式
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //kafka数据values的序列化方式
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //kafka 生产者配置源码类 ProducerConfig

        //创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        //循坏发送数据，i作为数据的value值
        for (int i = 0; i < 10; i++)
            producer.send(new ProducerRecord<String, String>("second", String.valueOf(i)), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println(metadata.partition() + "-----" + metadata.offset());
                    } else {
                        System.out.println("发送失败");
                    }
                }
            });

        //关闭资源
        producer.close();
    }
}
