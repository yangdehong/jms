package com.ydh.redsheep.selfrabbitmq.qmqp;

import com.rabbitmq.client.*;

import java.io.IOException;
/**
* 推的模式
* @author : yangdehong
* @date : 2021/2/5 13:48
*/
public class HelloPushConsumer {
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri("amqp://root:123456@172.16.131.16:5672/%2f");

        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();
        // 确保MQ中有该队列，如果没有则创建
        channel.queueDeclare("queue.biz", false, false, true, null);

        // 监听消息，一旦有消息推送过来，就调用第一个lambda表达式
        channel.basicConsume("queue.biz", (consumerTag, message) -> {
            System.out.println(new String(message.getBody()));
        }, (consumerTag) -> {});

//        channel.close();
//        connection.close();
    }
}
