package com.ydh.redsheep.selfrocket.order.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class OrderConsumer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer_grp_11_01");
        consumer.setNamesrvAddr("172.16.131.16:9876");
        consumer.subscribe("tp_demo_11", "*");

        consumer.setConsumeThreadMin(1);
        consumer.setConsumeThreadMax(1);
        consumer.setPullBatchSize(1);
        consumer.setConsumeMessageBatchMaxSize(1);

        // 使用有序消息监听器
        consumer.setMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {

                for (MessageExt msg : msgs) {
                    System.out.println(
                            msg.getTopic() + "\t" +
                            msg.getQueueId() + "\t" +
                            new String(msg.getBody())
                    );
                }

                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        consumer.start();

    }
}
