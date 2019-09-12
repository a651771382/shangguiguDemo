package com.lym.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author:李雁敏
 * @create:2019-09-03 10:23
 */
public class PartitionProducer {
    public static void main(String[] args) {
        //1.创建kafka生产者的信息
        Properties properties = new Properties();
        //添加配置信息
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ceshi:9092"); //kafka集群地址
        properties.put(ProducerConfig.ACKS_CONFIG, "-1");//ACK机制应答级别
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);//重试次数
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);//每个批次的大小
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1); //等待的时间
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432); //RecordAccumulator缓冲区大小
        //数据key的序列化方式
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //数据value的序列化方式
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.lym.partition.MyPartitioner");

        //创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

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

            //关闭资源
            producer.close();
        }
    }
}
