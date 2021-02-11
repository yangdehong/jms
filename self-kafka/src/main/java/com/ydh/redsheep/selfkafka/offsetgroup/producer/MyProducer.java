package com.ydh.redsheep.selfkafka.offsetgroup.producer;

public class MyProducer {
    public static void main(String[] args){
        Thread thread = new Thread(new ProducerHandler("hello lagou "));
        thread.start();
    }
}