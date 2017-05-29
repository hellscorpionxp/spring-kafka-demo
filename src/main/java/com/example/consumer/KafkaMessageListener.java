/**
 * Copyright(c) 2004-2017 by qyer.com All rights reserved.
 */
package com.example.consumer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;

/**
 * @author hellscorpion
 * @date 2017-05-29
 * @version 1.0
 */
public class KafkaMessageListener<K, V> {

    private KafkaMessageListenerContainer<K, V> container;

    public KafkaMessageListener(ContainerProperties containerProps) {
        Map<String, Object> props = consumerProps();
        DefaultKafkaConsumerFactory<K, V> cf = new DefaultKafkaConsumerFactory<>(
            props);
        this.container = new KafkaMessageListenerContainer<>(cf,
            containerProps);
    }

    public void setBeanName(String name) {
        container.setBeanName(name);
    }

    public void start() {
        container.start();
    }

    public void stop() {
        container.stop();
    }

    private Map<String, Object> consumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "foo");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class);
        return props;
    }

}
