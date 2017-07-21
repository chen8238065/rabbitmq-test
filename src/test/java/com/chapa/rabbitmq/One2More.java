package com.chapa.rabbitmq;

import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.QueueingConsumer;
import org.junit.Test;

/**
 * Created by chapa on 16-8-19.
 */
public class One2More extends  TestBase{

    private static final String TASK_QUEUE_NAME = "task_queue";

    @Test
    public void send() throws Exception {
        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

        String message = "Hello RabbitMQ";

        channel.basicPublish("", TASK_QUEUE_NAME,
                MessageProperties.PERSISTENT_TEXT_PLAIN,
                message.getBytes());
        System.out.println(" [x] Sent '" + message + "'");
    }

    @Test
    public void receive() throws Exception {
        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        //保证在接收端一个消息没有处理完时不会接收另一个消息，即接收端发送了ack后才会接收下一个消息。在这种情况下发送端会尝试把消息发送给下一个not busy的接收端。
        channel.basicQos(1);

        QueueingConsumer consumer = new QueueingConsumer(channel);
        //autoAck为false，即不自动会发ack，由channel.basicAck()在消息处理完成后发送消息。
        channel.basicConsume(TASK_QUEUE_NAME, false, consumer);

        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = new String(delivery.getBody());

            System.out.println(" [x] Received '" + message + "'");
            doWork(message);
            System.out.println(" [x] Done");

            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        }
    }

    private static void doWork(String task) throws InterruptedException {
        for (char ch : task.toCharArray()) {
            if (ch == '.') Thread.sleep(1000);
        }
    }

}
