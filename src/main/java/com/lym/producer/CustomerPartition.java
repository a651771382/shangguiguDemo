package com.lym.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author:李雁敏
 * @create:2019-08-27 16:38
 */
public class CustomerPartition implements Partitioner {
    private Map configMap = null;

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return 0;
    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {
        configMap=configs;
    }
}
