package com.ydh.redsheep.selfrocket.demo.produer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

public class MyProducer {

    public static void main(String[] args) throws UnsupportedEncodingException, InterruptedException, RemotingException, MQClientException, MQBrokerException {
        // 该producer是线程安全的，可以多线程使用。
        // 建议使用多个Producer实例发送
        // 实例化生产者实例，同时设置生产组名称
        DefaultMQProducer producer = new DefaultMQProducer("myproducer_grp_01");
        // 设置实例名称。一个JVM中如果有多个生产者，可以通过实例名称区分
        // 默认DEFAULT
        producer.setInstanceName("myproducer_grp_01");
        // 设置同步发送重试的次数
        producer.setRetryTimesWhenSendFailed(2);
        // 指定NameServer的地址
        producer.setNamesrvAddr("172.16.131.16:9876");
        // 对生产者进行初始化，然后就可以使用了
        producer.start();
        // 创建消息，第一个参数是主题名称，第二个参数是消息内容
        Message message = new Message(
                "tp_demo_01",
                "hello ydh 01".getBytes(RemotingHelper.DEFAULT_CHARSET)
        );
        // 同步发送消息，如果消息发送失败，则按照setRetryTimesWhenSendFailed设置的次数进行重试
        // broker中可能会有重复的消息，由应用的开发者进行处理
        final SendResult result = producer.send(message, 60000);
        System.out.println(result);
        // 关闭生产者
        producer.shutdown();
    }


}
