package com.chapa.rabbitmq;

import com.rabbitmq.client.QueueingConsumer;
import org.junit.Test;

/**
 * 广播通知所有订阅了该路由的消费者  不需要route key
 * {@link com.chapa.core.util.rabbitmq}
 * Created by chapa on 16-8-19.
 */
public class FanoutTest extends  TestBase{

    private static final String EXCHANGE_NAME = "amq.fanout";

    @Test
    public void send() throws Exception {
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout", true);

        for (int i = 0; i <100 ; i++) {
            String message = "Hello RabbitMQ!"+i;
            channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");
        }
    }

    @Test
    public void receive() throws Exception {
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout", true);
        //得到一个随机名称的Queue，该queue的类型为non-durable、exclusive、auto-delete的，将该queue绑定到上面的exchange上接收消息。
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, "");

        System.out.println(" [*] receive Waiting for messages. To exit press CTRL+C");

        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(queueName, true, consumer);

        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = new String(delivery.getBody());

            System.out.println(" [x]receive Received '" + message + "'");
        }
    }

    @Test
    public void receive1() throws Exception {

        channel.exchangeDeclare(EXCHANGE_NAME, "fanout", true);
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, "");

        System.out.println(" [*] receive1 Waiting for messages. To exit press CTRL+C");

        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(queueName, true, consumer);

        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = new String(delivery.getBody());

            System.out.println(" [x] receive1 Received '" + message + "'");
        }
    }
}
