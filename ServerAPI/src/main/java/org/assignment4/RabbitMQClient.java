package org.assignment4;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

public class RabbitMQClient {
    private static final String QUEUE_NAME = "skier_records";
    private static final int POOL_SIZE = 300;  // Number of pre-created channels
    private final Connection connection;
    private final BlockingQueue<Channel> channelPool;

    public RabbitMQClient(String host, String username, String password) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setUsername(username);
        factory.setPassword(password);
        this.connection = factory.newConnection();

        // Initialize channel pool
        this.channelPool = new ArrayBlockingQueue<>(POOL_SIZE);
        for (int i = 0; i < POOL_SIZE; i++) {
            Channel channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            channelPool.offer(channel);
        }
    }

    public void publishMessage(String message) throws IOException, InterruptedException {
        Channel channel = channelPool.take();
        try {
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
        } finally {
            channelPool.offer(channel);
        }
    }

    public void close() throws IOException, TimeoutException {
        for (Channel channel : channelPool) {
            channel.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
