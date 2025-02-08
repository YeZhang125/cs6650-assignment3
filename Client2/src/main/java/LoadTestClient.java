import java.io.BufferedWriter;
import java.io.FileWriter;
import java.net.URI;
import java.net.http.*;
import java.net.http.HttpResponse.BodyHandlers;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Random;

public class LoadTestClient {
    private static final int TOTAL_REQUESTS = 200000;
    private static final int INITIAL_THREADS = 32;
    private static final int INITIAL_BATCH_SIZE = 1000;
    private static final int MAX_THREADS = 100;
    private static final int MAX_RETRIES = 5;
    private static final String SERVER_URL = "http://52.88.171.176:8080/assignment1_war/skiers";

    private static final BlockingQueue<String> eventQueue = new LinkedBlockingQueue<>(5000);
    private static final AtomicInteger successfulRequests = new AtomicInteger(0);
    private static final AtomicInteger failedRequests = new AtomicInteger(0);
    private static final AtomicInteger remainingRequests = new AtomicInteger(TOTAL_REQUESTS);
    private static final Queue<Long> latencies = new ConcurrentLinkedQueue<>();


    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        // Start event producer
        Thread producerThread = new Thread(new EventProducer());
        producerThread.start();

        // Use a fixed-size thread pool for predictable concurrency
        ExecutorService executor = Executors.newFixedThreadPool(MAX_THREADS);

        // Start initial workers
        for (int i = 0; i < INITIAL_THREADS; i++) {
            executor.execute(new PostWorker(INITIAL_BATCH_SIZE));
        }

        // Dynamically add more threads as needed
        while (remainingRequests.get() > 0) {
            if (((ThreadPoolExecutor) executor).getActiveCount() < MAX_THREADS) {
                int batchSize = Math.min(INITIAL_BATCH_SIZE, remainingRequests.get());
                executor.execute(new PostWorker(batchSize));
            }
        }

        // Shutdown and wait for completion
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.MINUTES)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }

        long endTime = System.currentTimeMillis();
        double totalTimeSeconds = (endTime - startTime) / 1000.0;
        double throughput = TOTAL_REQUESTS / totalTimeSeconds;

        // Calculate latency statistics after all requests are processed
        double meanLatency = calculateMeanLatency();
        double medianLatency = calculateMedianLatency();
        long p99Latency = calculatePercentileLatency(99);
        long minLatency = 0;
        long maxLatency = 0;
        List<Long> sortedLatencies = new ArrayList<>(latencies);
        if (!sortedLatencies.isEmpty()) {
            sortedLatencies.sort(Long::compare);
            minLatency = sortedLatencies.get(0);
            maxLatency = sortedLatencies.get(sortedLatencies.size() - 1);
        }

        // Output the results
        System.out.println("Successful requests: " + successfulRequests.get());
        System.out.println("Failed requests: " + failedRequests.get());
        System.out.println("Total time: " + totalTimeSeconds + " seconds");
        System.out.println("Throughput: " + throughput + " requests/sec");
        System.out.println("Mean response time: " + meanLatency + " ms");
        System.out.println("Median response time: " + medianLatency + " ms");
        System.out.println("p99 response time: " + p99Latency + " ms");
        System.out.println("Min response time: " + minLatency + " ms");
        System.out.println("Max response time: " + maxLatency + " ms");

    }

    static class EventProducer implements Runnable {
        private final Random random = new Random();

        @Override
        public void run() {
            while (remainingRequests.get() > 0) {
                try {
                    String event = generateEvent();
                    eventQueue.put(event); // Blocks if queue is full
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        private String generateEvent() {
            int skierID = random.nextInt(100000) + 1;
            int resortID = random.nextInt(10) + 1;
            int liftID = random.nextInt(40) + 1;
            int time = random.nextInt(360) + 1;
            return String.format("{\"skierID\":%d, \"resortID\":%d, \"liftID\":%d, \"seasonID\":2025, \"dayID\":1, \"time\":%d}",
                    skierID, resortID, liftID, time);
        }
    }

    static class PostWorker implements Runnable {
        private final int batchSize;
        private final HttpClient client = HttpClient.newHttpClient();
        private static final String LOG_FILE = "request_log.csv";
        public PostWorker(int batchSize) {
            this.batchSize = batchSize;
        }

        @Override
        public void run() {
            for (int i = 0; i < batchSize; i++) {
                System.out.println("Thread " + Thread.currentThread().getName() + " is processing task " + (i + 1));
                try {
                    String event = eventQueue.poll(2, TimeUnit.SECONDS);
                    if (event == null) break; // Exit if no event is available

                    boolean success = sendPostRequest(event);
                    if (success) {
                        successfulRequests.incrementAndGet();
                    } else {
                        failedRequests.incrementAndGet();
                    }
                    remainingRequests.decrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        private boolean sendPostRequest(String event) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(SERVER_URL))
                    .POST(HttpRequest.BodyPublishers.ofString(event))
                    .header("Content-Type", "application/json").timeout(Duration.ofSeconds(5))
                    .build();

            int attempts = 0;
            while (attempts < MAX_RETRIES) {
                try {
                    long startTimeStamp = System.currentTimeMillis();

                    HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
                    long endTimeStamp = System.currentTimeMillis();

                    long latency = endTimeStamp - startTimeStamp;

                    latencies.add(latency);

                    //
                    logRequestData(startTimeStamp, "POST", latency, response.statusCode());
                     if (response.statusCode() == 201) {
                        System.out.println("Response: " + response.statusCode() + response.body());
                        return true;
                    } else {
                        System.err.println("Error " + response.statusCode() + ": " + response.body());
                    }
                } catch (IOException | InterruptedException e) {
                    System.err.println("Request failed: Retrying (" + (attempts + 1) + "/" + MAX_RETRIES + ")");
                }
                attempts++;
            }
            return false;
        }

        private void logRequestData(long startTimestamp, String post, long latency, int i) {

            String[] data = new String[]{
                    String.valueOf(startTimestamp), // start timestamp
                    post,                           // POST request type (or URL)
                    String.valueOf(latency),        // latency in milliseconds
                    String.valueOf(i)    // response code
            };

            // Write the data to the CSV file
            writeToCSV(data);
        }

        private void writeToCSV(String[] data) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(LOG_FILE, true))) {
                // Write the data row to the file
                writer.write(String.join(",", data)); // Join array elements with commas
                writer.newLine(); // Move to the next line for the next entry
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static double calculateMeanLatency() {
        return latencies.stream().mapToLong(Long::longValue).average().orElse(0);
    }

    private static double calculateMedianLatency() {
        List<Long> sortedLatencies = new ArrayList<>(latencies);
        if (sortedLatencies.isEmpty()) return 0;

        sortedLatencies.sort(Long::compare);
        int size = sortedLatencies.size();
        if (size % 2 == 0) {
            return (sortedLatencies.get(size / 2 - 1) + sortedLatencies.get(size / 2)) / 2.0;
        } else {
            return sortedLatencies.get(size / 2);
        }
    }

    private static long calculatePercentileLatency(int percentile) {
        List<Long> sortedLatencies = new ArrayList<>(latencies);
        if (sortedLatencies.isEmpty()) return 0;

        sortedLatencies.sort(Long::compare);
        int index = Math.min((int) (sortedLatencies.size() * (percentile / 100.0)), sortedLatencies.size() - 1);
        return sortedLatencies.get(index);
    }

}
