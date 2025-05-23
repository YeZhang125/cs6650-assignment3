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
    // private static final String HOST = "54.203.65.33";
    // private static final String QUEUE_NAME = "skier_queue";
    private static final String HOST = "54.191.39.166";
    private static final String QUEUE_NAME = "skier_records";
    private static final int THREAD_COUNT = 5;
    private static final int PREFETCH_COUNT = 100;
    private static final   String USERNAME = "myuser";
    private static final String PASSWORD = "mypassword";
    private Connection connection;
    private static ExecutorService executorService;
    // private static final String DBHost = "54.213.220.110";
    private static final String DBHost = "35.94.253.133";
    private static final JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), DBHost , 6379);
    private static final ExecutorService redisExecutor = Executors.newFixedThreadPool(10);

    public static void main(String[] args) throws Exception {

        // Initialize the connection pool

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
        mainChannel.queueDeclare(QUEUE_NAME, true, false, false, null);

        //   Start multiple consumers per queue

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
        } catch (InterruptedException | IOException e) {
            System.err.println("Error during shutdown: " + e.getMessage());
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

                channel.basicQos(PREFETCH_COUNT); // Limit to one unacknowledged message per consumer

                System.out.println(" [*] Waiting for messages in " + queueName);

                channel.basicConsume(queueName, false, (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
//                    System.out.println(" [x] Received from " + queueName + ": " + message);

                    try {
                        storeMessage(message);  // Process message
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);  // Acknowledge
                    } catch (Exception e) {
                        System.err.println("Error processing message: " + e.getMessage());
                        // Nack the message and do not requeue
                        channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, false);
                    }

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
            CompletableFuture.runAsync(() -> {
                try(Jedis jedis = jedisPool.getResource()){
                    JsonObject jsonObject =  JsonParser.parseString(message).getAsJsonObject();
                    int skierID = jsonObject.get("skierID").getAsInt();
                    int liftID = jsonObject.get("liftID").getAsInt();
                    int resortID = jsonObject.get("resortID").getAsInt();
                    int dayID = Integer.parseInt(jsonObject.get("dayID").getAsString());
                    int seasonID = Integer.parseInt(jsonObject.get("seasonID").getAsString());
                    int time = Integer.parseInt(jsonObject.get("time").getAsString());

                    System.out.println("Parsed seasonID = " + seasonID);
                    System.out.println("Writing to resort key: resort:" + resortID + ":season:" + seasonID + ":day:" + dayID + ":skiers");

                    // For skier N, how many days have they skied this season?
                    // Increment the number of days skied for the current season by 1
                    // GET skier:123:2025:days_skied
                    String skierSeasonKey = "skier:" + skierID + ":" + seasonID + ":days_skied";
                    jedis.incrBy(skierSeasonKey, 1);


                    // For skier N, show me the lifts they rode on each ski day
                    // HGETALL skier:<skierID>:<seasonID>:<dayID>
                    String skierKey = "skier:" + skierID + ":" + seasonID + ":" + dayID;
                    String liftIDKey =  "liftID: " + liftID;
                    String timeKey =  "time: " + time;
                    jedis.hset(skierKey,liftIDKey, timeKey);


                    // For skier N, what are the vertical totals for each ski day
                    // HGETALL skier:123:vertical.
                    // This would return a list of all fields and their corresponding vertical totals
                    String verticalKey = "skier:" + skierID + ":vertical";
                    String field = "day:" + dayID;
                    jedis.hincrBy(verticalKey,field, liftID * 10);




                    // Modified
                    // Supports API: GET /skiers/{skierID}/vertical?resort={resortID}
                    // Enables efficient querying of vertical totals for a specific resort
                    String verticalByResortKey = "skier:" + skierID + ":vertical:byResort";
                    String resortField = "resort:" + resortID;
                    jedis.hincrBy(verticalByResortKey, resortField, liftID * 10);

                    // Supports API: GET /skiers/{skierID}/vertical?resort={resortID}&season={seasonID}
                    // Tracks vertical totals for a specific resort and season combination
                    String verticalByResortSeasonKey = "skier:" + skierID + ":vertical:byResortSeason";
                    String resortSeasonField = "resort:" + resortID + ":season:" + seasonID;
                    jedis.hincrBy(verticalByResortSeasonKey, resortSeasonField, liftID * 10);

                    // Supports API: GET /resorts/{resortID}/seasons/{seasonID}/day/{dayID}/skiers
                    // Supports Query: "How many unique skiers visited resort X on day N?"
                    // Tracks unique skiers at a specific resort on a specific day in a season
                    String resortKeyWithSeason = "resort:" + resortID + ":season:" + seasonID + ":day:" + dayID + ":skiers";
                    jedis.sadd(resortKeyWithSeason, String.valueOf(skierID));



                }catch (Exception e ){
                    System.err.println("Error getting Redis connection: " + e.getMessage());
                    e.printStackTrace();
                }
            }, redisExecutor);
        }

    }
}