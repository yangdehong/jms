package com.ydh.redsheep.selfrabbitmq.fanout;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Consumer2 {
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri("amqp://root:123456@172.16.131.16:5672/%2f");

        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        final String queueName = channel.queueDeclare().getQueue();
        System.out.println("生成的临时队列的名字为：" + queueName);

        channel.exchangeDeclare("ex.myfan",
                BuiltinExchangeType.FANOUT,
                true,
                false,
                null);

        // fanout类型的交换器绑定不需要routingkey
        channel.queueBind(queueName, "ex.myfan", "");

        channel.basicConsume(queueName, (consumerTag, message) -> {
            System.out.println("Two   " + new String(message.getBody(), "utf-8"));
        }, consumerTag -> {});

    }
}
