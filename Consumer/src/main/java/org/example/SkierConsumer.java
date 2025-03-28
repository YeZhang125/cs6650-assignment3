
package org.example;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rabbitmq.client.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;

public class SkierConsumer {
    private static final String HOST = "34.217.212.191";
    private static final String QUEUE_NAME = "skier_queue";
    private static final String RESPONSE_QUEUE = "skier_response_queue"; // New response queue
    private static final int THREAD_COUNT = 5;
    private static final int PREFETCH_COUNT = 100;
    private static final String USERNAME = "test_user";
    private static final String PASSWORD = "test_password";
    private Connection connection;
    private static ExecutorService executorService;
    private static final String DBHost = "44.247.153.135";
    private static final JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), DBHost , 6379);

    public static void main(String[] args) throws Exception {
        SkierConsumer consumer = new SkierConsumer();
        consumer.startConsuming();

        // Graceful shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down consumers...");
            try {
                consumer.stopConsuming();  // Ensure graceful shutdown of consumers
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }));
    }

    public void startConsuming() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setUsername(USERNAME);
        factory.setPassword(PASSWORD);
        factory.setAutomaticRecoveryEnabled(true);
        factory.setRequestedHeartbeat(30);

        // Establish a single connection
        connection = factory.newConnection();

        // Thread pool for consumers
        executorService = Executors.newFixedThreadPool(THREAD_COUNT);

        // Main thread - Declare queue only once before launching threads
        Channel mainChannel = connection.createChannel();
        // Declare the response queue only once
        mainChannel.queueDeclare(RESPONSE_QUEUE, true, false, false, null);
        // Declare the main queue and start multiple consumers
        mainChannel.queueDeclare(QUEUE_NAME, true, false, false, null);

        // Start multiple consumers per queue
        for (int i = 0; i < THREAD_COUNT; i++) {
            executorService.submit(new ConsumerTask(connection, QUEUE_NAME));
        }
    }

    public void stopConsuming() throws Exception {
        try {
            if (executorService != null) {
                executorService.shutdown();
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            }
            if (connection != null) {
                connection.close();
            }
            clearRedisData();
        } catch (InterruptedException | IOException e) {
            System.err.println("Error during shutdown: " + e.getMessage());
        }
    }

    public void clearRedisData() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.flushAll();
            System.out.println("All Redis data cleared.");
        } catch (Exception e) {
            System.err.println("Error clearing Redis data: " + e.getMessage());
        }
    }

    // Consumer task for multi-threading
    private static class ConsumerTask implements Runnable {

        private final Connection connection;
        private final String queueName;

        public ConsumerTask(Connection connection, String queueName) {
            this.connection = connection;
            this.queueName = queueName;
        }

        @Override
        public void run() {

            try {
                // Create a new channel
              Channel  channel = connection.createChannel();
                // Set QoS (Quality of Service)
                channel.basicQos(PREFETCH_COUNT);

                // Declare the queue
                channel.queueDeclare(queueName, true, false, false, null);
                System.out.println(" [*] Waiting for messages in " + queueName);

                // Consume messages

                channel.basicConsume(queueName, false, (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                    String correlationId = delivery.getProperties().getCorrelationId();
                    System.out.println(" [x] Received from " + queueName + ": " + message);

                    try {
                        boolean processedSuccessfully = storeMessage(message);  // Process message

                        // Acknowledge only if processing is successful
                        if (processedSuccessfully) {
                            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                        } else {
                            channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, false);
                        }
                    } catch (Exception e) {
                        System.err.println("Error processing message: " + e.getMessage());

                        // Handle the error and nack the message if needed
                        try {
                            channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, false);
                        } catch (IOException ioException) {
                            System.err.println("Error acknowledging message: " + ioException.getMessage());
                        }
                    }
                }, consumerTag -> {
                    System.out.println("Cancelled consumption of queue: " + queueName);
                });

                // Wait indefinitely for incoming messages
                synchronized (this) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

            } catch (IOException e) {
                System.err.println("Error with RabbitMQ connection or channel: " + e.getMessage());
            }
        }
        private boolean storeMessage(String message) {
            try (Jedis jedis = jedisPool.getResource()) {
                JsonObject jsonObject = JsonParser.parseString(message).getAsJsonObject();
                int skierID = jsonObject.get("skierID").getAsInt();
                int liftID = jsonObject.get("liftID").getAsInt();
                int resortID = jsonObject.get("resortID").getAsInt();
                int dayID = jsonObject.get("dayID").getAsInt();
                int seasonID = jsonObject.get("seasonID").getAsInt();
                int time = jsonObject.get("time").getAsInt();

                // For skier N, how many days have they skied this season?
                String skierSeasonKey = "skier:" + skierID + ":" + seasonID + ":days_skied";
                jedis.incrBy(skierSeasonKey, 1);

                // For skier N, show me the lifts they rode on each ski day
                String skierKey = "skier:" + skierID + ":" + seasonID + ":" + dayID;
                String liftIDKey = "liftID: " + liftID;
                String timeKey = "time: " + time;
                jedis.hset(skierKey, liftIDKey, timeKey);

                // For skier N, what are the vertical totals for each ski day
                String verticalKey = "skier:" + skierID + ":vertical";
                String field = "day:" + dayID;
                jedis.hincrBy(verticalKey, field, liftID * 10);

                // How many unique skiers visited resort X on day N
                String resortKey = "resort:" + resortID + ":day:" + dayID + ":skiers";
                jedis.sadd(resortKey, "The total number of unique skiers: " + String.valueOf(skierID));

                return true;
            } catch (Exception e) {
                System.err.println("Error getting Redis connection: " + e.getMessage());
                return false;
            }
        }

    }
}
