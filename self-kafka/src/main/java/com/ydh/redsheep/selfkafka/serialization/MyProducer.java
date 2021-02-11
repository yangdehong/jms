package com.ydh.redsheep.selfkafka.serialization;

import com.ydh.redsheep.selfkafka.pojo.User;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

public class MyProducer {
    public static void main(String[] args) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.131.16:9092");
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // 设置自定义的序列化器
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserSerializer.class);

        KafkaProducer<String, User> producer = new KafkaProducer<>(configs);

        User user = new User();
//        user.setUserId(1001);
//        user.setUsername("张三");
//        user.setUsername("李四");
//        user.setUsername("王五");
        user.setUserId(400);
        user.setUsername("赵四");

        ProducerRecord<String, User> record = new ProducerRecord<>(
                "tp_user_01",   // topic
                user.getUsername(),   // key
                user                  // value
        );

        producer.send(record, (RecordMetadata metadata, Exception exception) -> {
            if (exception != null) {
                System.out.println("消息发送异常");
            } else {
                System.out.println("主题：" + metadata.topic() + "\t"
                        + "分区：" + metadata.partition() + "\t"
                        + "生产者偏移量：" + metadata.offset());
            }
        });

        // 关闭生产者
        producer.close();

    }
}
