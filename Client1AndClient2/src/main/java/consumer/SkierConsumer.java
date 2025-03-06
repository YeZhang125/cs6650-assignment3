package consumer;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.rabbitmq.client.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.*;

public class SkierConsumer {
    private static final String QUEUE_NAME = "skier_queue";
    private static final String HOST = "35.89.251.156";
    private static final int THREAD_COUNT = 10;

    private static final int MAX_QUEUE_SIZE = 10000;  // Max messages to process at once

    private static final Map<Integer, ConcurrentLinkedQueue<JsonObject>> skierData = new ConcurrentHashMap<>();
    private static final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);

    public static void main(String[] args) throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setPort(5672);
        factory.setUsername("test_user");
        factory.setPassword("test_password");

        // Enable automatic recovery
        factory.setAutomaticRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(5000);  // Retry interval in case of connection failure

        Connection connection = null;
        while (connection == null) {
            try {
                connection = factory.newConnection();
                System.out.println("Connected to RabbitMQ on EC2. Spawning worker threads...");
            } catch (IOException e) {
                System.out.println("Connection failed, retrying...");
                Thread.sleep(5000); // Retry after 5 seconds
            }
        }

        // Create multiple consumers (threads) to process messages
        for (int i = 0; i < THREAD_COUNT; i++) {
            final Channel channel = connection.createChannel();
            channel.basicQos(10);  // Limit to 100 unacknowledged message per consumer

            Thread consumerThread = new Thread(() -> {
                try {
                    channel.queueDeclare(QUEUE_NAME, true, false, false, null);
                    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                        String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                        executorService.submit(() -> processMessage(message));
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    };

                    boolean autoAck = false;
                    channel.basicConsume(QUEUE_NAME, autoAck, deliverCallback, consumerTag -> {});

                    // Add shutdown listener to handle connection closures
                    channel.addShutdownListener(cause -> {
                        System.out.println("Channel shutdown: " + cause.getMessage());
                        if (!cause.isInitiatedByApplication()) {
                            System.out.println("Attempting to reconnect...");
                            try {
                                // Automatic recovery will handle reconnections
                             } catch (Exception e) {
                                System.out.println("Reconnection failed: " + e.getMessage());
                            }
                        }
                    });

                } catch (IOException e) {
                    System.out.println("IOException occurred while consuming messages: " + e.getMessage());
                }
            });

            consumerThread.start();
        }
    }

    private static void processMessage(String message) {
        if (skierData.size() > MAX_QUEUE_SIZE) {
            // Simulate backpressure
            try {
                int processingTime = (int) (Math.random() * 100);  // Random delay between 0-100ms
                Thread.sleep(processingTime);
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }
            return;
        }
        Gson gson = new Gson();
        JsonObject jsonObject = gson.fromJson(message, JsonObject.class);

        int skierID = jsonObject.get("skierID").getAsInt();
        skierData.computeIfAbsent(skierID, k -> new ConcurrentLinkedQueue<>()).add(jsonObject);
    }
}
