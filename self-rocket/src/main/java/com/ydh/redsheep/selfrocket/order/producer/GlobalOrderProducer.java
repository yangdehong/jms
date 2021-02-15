package com.ydh.redsheep.selfrocket.order.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
* 全局有序
* @author : yangdehong
* @date : 2021/2/14 16:49
*/
public class GlobalOrderProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("producer_grp_11_02");
        producer.setNamesrvAddr("172.16.131.16:9876");

        producer.start();

        Message message = null;

        for (int i = 0; i < 100; i++) {
            message = new Message("tp_demo_11_01", ("hello lagou" + i).getBytes());
            producer.send(message, 60000);
        }

        producer.shutdown();
    }
}