package com.ydh.redsheep.selfrocket.loadbalance.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
/**
 * 手动指定发送队列信息，手动实现负载均衡
 * @author : yangdehong
 * @date : 2021/2/14 14:45
 */
public class MyConsumer {
    public static void main(String[] args) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("consumer_grp_06_01");
        consumer.setNamesrvAddr("172.16.131.16:9876");

        consumer.start();

        final PullResult pullResult = consumer.pull(new MessageQueue(
                        "tp_demo_06",
                        "node1",
                        4
                ),
                "*",
                0L,
                10);

        System.out.println(pullResult);

        pullResult.getMsgFoundList().forEach(messageExt -> {
            System.out.println(messageExt);
        });

        consumer.shutdown();

    }
}
