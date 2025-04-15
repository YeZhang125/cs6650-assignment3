import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


public class MultiThreadedLiftRideClient {
    private static final String SERVER_URL = "http://cs6650-369107167.us-west-2.elb.amazonaws.com/connect_to_rmq";
//    private static final String SERVER_URL = "http://35.166.130.191:8080/assignment1_war";
    private static final int TOTAL_REQUESTS = 200000;
    private static final int NUMBER_OF_THREADS = 200;
    private static final int REQUEST_PER_THREAD = 1000;
    private static final int REQUEST_PER_INITIAL_THREAD = 1000;
    private static final int PHASE1_THREAD = 32;

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        // Run client tests for day values 1, 2, and 3
        for (int day = 1; day <= 3; day++) {
            System.out.println("\n========= STARTING RUN FOR DAY " + day + " =========\n");
            runClientTest(day);

            // Brief pause between test runs
            if (day < 3) {
                System.out.println("\nPausing for 5 seconds before next run...\n");
                Thread.sleep(5000);
            }
        }
    }

    private static void runClientTest(int dayValue) throws InterruptedException, ExecutionException {
        AtomicInteger successfulCount = new AtomicInteger(0);
        AtomicInteger failedCount = new AtomicInteger(0);
        String logFileName = "request_logs_day_" + dayValue + ".csv";

        Path logFilePath = Paths.get(logFileName);
        try {
            if (Files.exists(logFilePath)) {
                Files.delete(logFilePath);
                System.out.println("Deleted " + logFilePath);
            }
        } catch (IOException e) {
            System.out.println("Error deleting csv file '" + logFileName + "': " + e.getMessage());
        }

        // Set the current day ID for this test run
        SkierLiftProducer.setCurrentDayID(dayValue);

        // Start event producer thread
        Thread eventProducerThread = new Thread(new EventProducer());
        eventProducerThread.start();

        ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
        List<Future<Void>> futures = new ArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        long startTime = System.currentTimeMillis();

        System.out.println("Starting 32 threads for day " + dayValue + "...");
        for (int i = 0; i < PHASE1_THREAD ; i++) {
            HTTPClientThread clientThread = new HTTPClientThread(SERVER_URL, successfulCount, failedCount, REQUEST_PER_INITIAL_THREAD, countDownLatch, logFileName);
            futures.add(executor.submit(clientThread));
        }
        countDownLatch.await();
        System.out.println("Finished 1 of 32 threads for day " + dayValue + "!");

        int remainingRequests = TOTAL_REQUESTS - (PHASE1_THREAD * REQUEST_PER_INITIAL_THREAD);
        int threadNeeded = remainingRequests / REQUEST_PER_THREAD;

        System.out.println("Starting additional threads for day " + dayValue + "...");

        for (int i = 0; i < threadNeeded; i++) {
            HTTPClientThread clientThread = new HTTPClientThread(SERVER_URL, successfulCount, failedCount, REQUEST_PER_THREAD, null, logFileName);
            futures.add(executor.submit(clientThread));
        }

        for (Future<Void> future : futures) {
            future.get();
        }

        eventProducerThread.join();
        executor.shutdown();

        long endTime = System.currentTimeMillis();
        long responseTime = endTime - startTime;

        System.out.println("======= Client Output for Day " + dayValue + " ======= ");
        System.out.println("Number of Threads: " + (threadNeeded + PHASE1_THREAD));
        System.out.println("Successful requests: " + successfulCount.get());
        System.out.println("Failed requests: " + failedCount.get());
        System.out.println("Total requests sent: " + successfulCount.get());
        System.out.println("Total response time: " + responseTime + " ms");
        System.out.println("Throughput: " + (TOTAL_REQUESTS / (responseTime / 1000.0)) + " requests per second");
    }
}