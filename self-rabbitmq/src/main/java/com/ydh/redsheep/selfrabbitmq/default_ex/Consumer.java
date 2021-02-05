package com.ydh.redsheep.selfrabbitmq.default_ex;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;

public class Consumer {
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri("amqp://root:123456@172.16.131.16:5672/%2f");

        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        final GetResponse getResponse = channel.basicGet("queue.default.ex", true);

        System.out.println("收到的消息：" + new String(getResponse.getBody(), "utf-8"));

        channel.close();
        connection.close();

    }
}
