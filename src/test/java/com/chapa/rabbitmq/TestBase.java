package com.chapa.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;

/**
 * Created by chapa on 17-7-21.
 */
public class TestBase {
    Logger logger = org.slf4j.LoggerFactory.getLogger("trace");
    Channel channel;
    Connection connection;
    @Before
    public void setUp() throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(Init.server);
        factory.setPort(Init.port);
        connection = factory.newConnection();
        channel = connection.createChannel();
    }

    @After
    public void close() throws Exception{
        channel.close();
        connection.close();
    }
}
