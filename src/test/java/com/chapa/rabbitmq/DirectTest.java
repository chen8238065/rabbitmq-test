package com.chapa.rabbitmq;

import com.rabbitmq.client.QueueingConsumer;
import org.junit.Test;

public class DirectTest  extends  TestBase{
    private static final String EXCHANGE_NAME = "nydus.143.im_unread_message_notify";
    private String routingKey="test";

    @Test
    public void send() throws Exception {
        channel.exchangeDeclare(EXCHANGE_NAME, "direct", true);

        /**
        * 要求路由键 “test”，则只有被标记为“test”的消息才被转发，不会转发test.aaa
        */
        for (int i = 0; i < 100; i++) {
            String message = "Hello RabbitMQ! " + this.routingKey +i;
            channel.basicPublish(EXCHANGE_NAME, this.routingKey, null, message.getBytes());
            System.out.println(" [x] Sent '" + this.routingKey + "':'" + message + "'");
        }
    }

    @Test
    public void receive() throws Exception {
        //Exchange 必须与服务端一致
        channel.exchangeDeclare(EXCHANGE_NAME, "direct", true);
        String queueName = channel.queueDeclare().getQueue();

        //绑定queue和exchange的时候使用了routing key，即从该exchange上只接收routing key指定的消息。
        channel.queueBind(queueName, EXCHANGE_NAME, this.routingKey);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(queueName, true, consumer);

        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = new String(delivery.getBody());
            String routingKey = delivery.getEnvelope().getRoutingKey();
            System.out.println(" [x] Received '" + routingKey + "':'" + message + "'");
            logger.info(" [x] Received '\" + routingKey + \"':'\" + message + \"'");
        }

    }
}
