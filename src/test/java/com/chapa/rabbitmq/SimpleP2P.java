package com.chapa.rabbitmq;

import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.QueueingConsumer;
import org.junit.Test;

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

    // 避免  QueueingConsumer  OOM
    @Test
    public void receive() throws Exception {
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
    public void pollget() throws Exception {
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        int i=0;
        while (true) {
            GetResponse response=channel.basicGet(QUEUE_NAME,false);
            System.out.println(response.toString());
        }
    }
}
