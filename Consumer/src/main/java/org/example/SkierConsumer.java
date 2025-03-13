 package org.example;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rabbitmq.client.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

 public class SkierConsumer {
    private static final String HOST = "34.222.11.231";
    private static final String QUEUE_NAME = "skier_queue";
    private static final int THREAD_COUNT = 5;
    private static final int PREFETCH_COUNT = 100;
    private Connection connection;
    private ExecutorService executorService;
    private static final ConcurrentHashMap<Integer, List<Integer>> messageStore = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        SkierConsumer consumer = new SkierConsumer();
        consumer.startConsuming();
    }

    public void startConsuming() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setUsername("test_user");
        factory.setPassword("test_password");
        factory.setAutomaticRecoveryEnabled(true);
        factory.setRequestedHeartbeat(30);
        // Establish a single connection
        connection = factory.newConnection();

        // Thread pool for consumers
        executorService = Executors.newFixedThreadPool(THREAD_COUNT);

      //   Start multiple consumers per queue

        for (int i = 0; i < THREAD_COUNT; i++) {
            executorService.submit(new ConsumerTask(connection, QUEUE_NAME));
        }

    }

    public void stopConsuming() throws Exception {
        if (executorService != null) {
            executorService.shutdown();
        }
        if (connection != null) {
            connection.close();
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
                Channel channel = connection.createChannel();
                channel.queueDeclare(queueName, true, false, false, null);
                channel.basicQos(PREFETCH_COUNT); // Limit to one unacknowledged message per consumer

                System.out.println(" [*] Waiting for messages in " + queueName);

                channel.basicConsume(queueName, false, (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                    System.out.println(" [x] Received from " + queueName + ": " + message);

                    storeMessage(message);
                    // Acknowledge message
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);



                }, consumerTag -> {
                    System.out.println("Cancelled consumption of queue: " + queueName);
                });

                synchronized (this) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            } catch (IOException e) {
                System.err.println(e.getMessage());
            }
        }

        private void storeMessage(String message) {
            try{
                JsonObject js =  JsonParser.parseString(message).getAsJsonObject();
                int skierID = js.get("skierID").getAsInt();
                int liftID = js.get("liftID").getAsInt();
                messageStore.computeIfAbsent(skierID, k-> Collections.synchronizedList(new ArrayList<>())).add(liftID);
            }catch (Exception e ){
               System.err.println(e.getMessage());
            }
        }
    }
}
