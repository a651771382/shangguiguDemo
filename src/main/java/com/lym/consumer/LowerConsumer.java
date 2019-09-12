package com.lym.consumer;

import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author:李雁敏
 * @create:2019-08-27 17:46
 * 低级API
 * <p>
 * 根据指定的Topic，partition，offset来获取数据
 */
public class LowerConsumer {
    public static void main(String[] args) {
        //定义相关参数
        ArrayList<String> brokers = new ArrayList<String>();//kafka集群
        brokers.add("ceshi");
        brokers.add("ceshi1");
        brokers.add("ceshi2");
        //端口号
        int port = 9092;
        //主题
        String topic = "second";
        //分区
        int partition = 0;
        //offset
        long offset = 2;

    }

    //找分区leader
    private String findLeader(List<String> brokers, int port, String topic, int partition) {
        for (String broker : brokers) {
            SimpleConsumer getLeader = new SimpleConsumer
                    (broker, port, 1000, 1024 * 4, "getLeader");
            //创建一个主题元数据信息请求
            TopicMetadataRequest topicMetadataRequest = new
                    TopicMetadataRequest(Collections.singletonList(topic));

            //获取主题元数据返回值
            TopicMetadataResponse send = getLeader.send(topicMetadataRequest);

            //解析元数据返回值
            List<TopicMetadata> topicMetadata = send.topicsMetadata();

            //遍历主题元数据
            for (TopicMetadata topicMetadata1 : topicMetadata) {
            }
        }

        return null;
    }

    //获取数据
    private void getData() {

    }
}
