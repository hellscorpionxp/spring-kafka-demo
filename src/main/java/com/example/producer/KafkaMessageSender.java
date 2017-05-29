/**
 * Copyright(c) 2004-2017 by qyer.com All rights reserved.
 */
package com.example.producer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

/**
 * @author hellscorpion
 * @date 2017-05-29
 * @version 1.0
 */
public class KafkaMessageSender<K, V> {

    private KafkaTemplate<K, V> template;

    public KafkaMessageSender() {
        Map<String, Object> senderProps = senderProps();
        ProducerFactory<K, V> pf = new DefaultKafkaProducerFactory<>(
            senderProps);
        this.template = new KafkaTemplate<>(pf);
    }

    public void setDefaultTopic(String defaultTopic) {
        template.setDefaultTopic(defaultTopic);
    }

    public ListenableFuture<SendResult<K, V>> sendDefault(K k, V v) {
        return template.sendDefault(k, v);
    }

    public void flush() {
        template.flush();
    }

    private Map<String, Object> senderProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class);
        return props;
    }

}
