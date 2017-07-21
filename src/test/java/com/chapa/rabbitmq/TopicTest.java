package com.chapa.rabbitmq;

import com.rabbitmq.client.QueueingConsumer;
import org.junit.Test;

/**
 * {@link com.chapa.core.util.rabbitmq}<br/>
 * Created by chapa on 16-8-19.
 */
public class TopicTest extends  TestBase{
    private static final String EXCHANGE_NAME = "topic_log";

    @Test
    public void send() {
        try {
            channel.exchangeDeclare(EXCHANGE_NAME, "topic", true,true,null);
            String routingKey = "hello.haha";
            String message = "Hello RabbitMQ! " + routingKey;
            channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes());
            System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void reveive() {
        try {
            channel.exchangeDeclare(EXCHANGE_NAME, "topic", true,true,null);
            String queueName = channel.queueDeclare().getQueue();
            /**
             * 符号“#”匹配一个或多个词，符号“*”匹配不多不少一个词。
             * “chapa.#”能够匹配到“chapa.irs.corporate”
             * “chapa.*” 只会匹配到“chapa.irs”
             */
            String[] arr = {"chapa.*", "chen.#", "hello.#", "hi.*"};
            for (String bindingKey : arr) {
                channel.queueBind(queueName, EXCHANGE_NAME, bindingKey);
            }

            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

            QueueingConsumer consumer = new QueueingConsumer(channel);
            channel.basicConsume(queueName, true, consumer);

            while (true) {
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                String message = new String(delivery.getBody());
                String routingKey = delivery.getEnvelope().getRoutingKey();

                System.out.println(" [x] Received '" + routingKey + "':'" + message + "'");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
