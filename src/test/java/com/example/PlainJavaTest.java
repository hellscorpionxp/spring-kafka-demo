/**
 * Copyright(c) 2004-2017 by qyer.com All rights reserved.
 */
package com.example;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;

import com.example.consumer.KafkaMessageListener;
import com.example.producer.KafkaMessageSender;

/**
 * @author hellscorpion
 * @date 2017-05-29
 * @version 1.0
 */
public class PlainJavaTest {

    @Test
    public void test() throws InterruptedException {
        ContainerProperties containerProps = new ContainerProperties("topic1",
            "topic2");
        final CountDownLatch latch = new CountDownLatch(4);
        containerProps
            .setMessageListener((MessageListener<Integer, String>) message -> {
                System.out.println(message);
                latch.countDown();
            });

        KafkaMessageListener<Integer, String> container = new KafkaMessageListener<>(
            containerProps);
        container.setBeanName("testAuto");
        container.start();
        Thread.sleep(1000);

        KafkaMessageSender<Integer, String> sender = new KafkaMessageSender<>();
        sender.setDefaultTopic("topic1");
        sender.sendDefault(0, "foo");
        sender.sendDefault(2, "bar");
        sender.sendDefault(0, "baz");
        sender.sendDefault(2, "qux");
        sender.flush();

        assertTrue(latch.await(60, TimeUnit.SECONDS));
        container.stop();
    }

}
