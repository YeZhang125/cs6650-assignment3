package consumer;

import com.rabbitmq.client.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SkierConsumer {
    private static final String HOST = "35.92.148.147";
    private static final String[] QUEUES = {
            "skier_queue_1"
    };
    private static final int THREAD_COUNT = 30;
    private static final int PREFETCH_COUNT = 2000;
    private Connection connection;
    private ExecutorService executorService;

    public static void main(String[] args) throws Exception {
        SkierConsumer consumer = new SkierConsumer();
        consumer.startConsuming();
    }

    public void startConsuming() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setUsername("test_user");
        factory.setPassword("test_password");

        // Establish a single connection
        connection = factory.newConnection();

        // Thread pool for consumers
        executorService = Executors.newFixedThreadPool(THREAD_COUNT);

        // Start a consumer thread for each queue
        for (String queueName : QUEUES) {
            executorService.submit(new ConsumerTask(connection, queueName));
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


                    // Acknowledge message
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }, consumerTag -> {
                    System.out.println("Cancelled consumption of queue: " + queueName);
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
