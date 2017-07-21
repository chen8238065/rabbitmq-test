package com.chapa.rabbitmq;

import com.rabbitmq.client.*;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by chapa on 16-8-19.
 */
public class SimpleP2P extends  TestBase {
    private final static String QUEUE_NAME = "hello";

    @Test
    public void sendMsg() throws Exception {
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        //不指定Exchange
        for (int i = 0; i < 100 ; i++) {
            String message = "Hello RabbitMQ!"+i;
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");
        }
    }

    //QueueingConsumer  维护一个本地队列 客户端异步处理推送过来的消息
    @Test
    public void receiveAsync() throws Exception {
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        // set Prefetch count 每次推送 1000条消息
        channel.basicQos(1000);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        QueueingConsumer consumer = new QueueingConsumer(channel);
        //取消 auto ack
        channel.basicConsume(QUEUE_NAME, false, consumer);

        int i=0;
        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = new String(delivery.getBody());
            System.out.println(" [x] Received '" + message + "'");

           // 手动发送 ack 消息
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        }
    }
    @Test
    public void syncReceive()throws Exception {
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        // set Prefetch count 每次推送 1000条消息
        channel.basicQos(1000);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        Consumer consumer =new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" [x] Received '" + message + "'");
            }
        };
        channel.basicConsume(QUEUE_NAME, true, consumer);
        Thread.currentThread().join();
    }

    @Test
    public void pollget() throws Exception {
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        int i=0;
        while (true) {
            GetResponse response=channel.basicGet(QUEUE_NAME,false);
            System.out.println(response.toString());
        }
    }
}
