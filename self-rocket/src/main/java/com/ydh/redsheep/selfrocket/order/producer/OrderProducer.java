package com.ydh.redsheep.selfrocket.order.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;

/**
* 单个消息队列消息有序
* @author : yangdehong
* @date : 2021/2/14 16:49
*/
public class OrderProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("producer_grp_11_01");

        producer.setNamesrvAddr("172.16.131.16:9876");

        producer.start();

        // 获取指定主题的MQ列表
        final List<MessageQueue> messageQueues = producer.fetchPublishMessageQueues("tp_demo_11");

        Message message = null;
        MessageQueue messageQueue = null;
        for (int i = 0; i < 100; i++) {
            // 采用轮询的方式指定MQ，发送订单消息，保证同一个订单的消息按顺序
            // 发送到同一个MQ
            messageQueue = messageQueues.get(i % 8);

            message = new Message("tp_demo_11", ("hello lagou order create - " + i).getBytes());
            producer.send(message, messageQueue, 60000);
            message = new Message("tp_demo_11", ("hello lagou order pay - " + i).getBytes());
            producer.send(message, messageQueue, 60000);
            message = new Message("tp_demo_11", ("hello lagou order ship - " + i).getBytes());
            producer.send(message, messageQueue, 60000);
        }

        producer.shutdown();
    }
}
