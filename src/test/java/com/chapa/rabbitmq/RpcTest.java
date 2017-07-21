package com.chapa.rabbitmq;

import com.rabbitmq.client.*;
import org.junit.Test;

/**
 * {@link com.chapa.core.util.rabbitmq}
 * Created by chapa on 16-8-19.
 */
public class RpcTest extends  TestBase{
    private static final String RPC_QUEUE_NAME = "rpc_queue";

    @Test
    public void server() throws Exception {

        channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
        channel.basicQos(1);

        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(RPC_QUEUE_NAME, false, consumer);

        System.out.println(" [x] Awaiting RPC requests");

        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();

            AMQP.BasicProperties props = delivery.getProperties();
            AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(props.getCorrelationId())
                    .build();

            String message = new String(delivery.getBody());
            int n = Integer.parseInt(message);

            System.out.println(" [.] fib(" + message + ")");
            String response = "" + fib(n);

            channel.basicPublish("", props.getReplyTo(), replyProps, response.getBytes());

            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        }
    }

    @Test
    public void client() throws Exception {
        RPCClient rpcClient = new RPCClient();

        System.out.println(" [x] Requesting fib(30)");
        String response = rpcClient.call("3");
        System.out.println(" [.] Got '" + response + "'");

        rpcClient.close();
    }

    private int fib(int n) throws Exception {
        if (n == 0) return 0;
        if (n == 1) return 1;
        return fib(n - 1) + fib(n - 2);
    }

    class RPCClient {
        private Connection connection;
        private Channel channel;
        private String requestQueueName = "rpc_queue";
        private String replyQueueName;
        private QueueingConsumer consumer;

        public RPCClient() throws Exception {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(Init.server);
            connection = factory.newConnection();
            channel = connection.createChannel();

            replyQueueName = channel.queueDeclare().getQueue();
            consumer = new QueueingConsumer(channel);
            channel.basicConsume(replyQueueName, true, consumer);
        }

        public String call(String message) throws Exception {
            String response = null;
            String corrId = java.util.UUID.randomUUID().toString();

            AMQP.BasicProperties props = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(corrId)
                    .replyTo(replyQueueName)
                    .build();

            channel.basicPublish("", requestQueueName, props, message.getBytes());

            while (true) {
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                    response = new String(delivery.getBody());
                    break;
                }
            }

            return response;
        }

        public void close() throws Exception {
            connection.close();
        }
    }


}
