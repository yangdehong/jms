package com.ydh.redsheep.selfrocket.loadbalance.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
* 手动指定发送队列信息，手动实现负载均衡
* @author : yangdehong
* @date : 2021/2/14 14:45
*/
public class MyProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("producer_grp_06_01");
        producer.setNamesrvAddr("172.16.131.16:9876");

        producer.start();
        // 第二个参数使用mqbroker -p|grep brokerName
        Message message = new Message("tp_demo_06", "hello lagou".getBytes());

        final SendResult result = producer.send(message, new MessageQueue("tp_demo_06", "node1", 3), 60000);
        System.out.println(result);

        producer.shutdown();

    }
}
