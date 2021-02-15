package com.ydh.redsheep.selfrocket.dlm.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class MyProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("producer_grp_09_01");
        producer.setNamesrvAddr("172.16.131.16:9876");
        producer.start();

        Message message = null;

        for (int i = 0; i < 10; i++) {
            message= new Message("tp_demo_09", ("hello lagou - " + i).getBytes());

            producer.send(message, 60000);

        }

        producer.shutdown();

    }
}
