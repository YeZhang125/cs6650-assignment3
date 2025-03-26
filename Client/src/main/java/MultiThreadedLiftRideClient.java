
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


public class MultiThreadedLiftRideClient {
    private static final String SERVER_URL = "http://35.94.9.239:8080/assignment1_war";
    private static final int TOTAL_REQUESTS = 200000;
    private static final int NUMBER_OF_THREADS = 200;
    private static final int REQUEST_PER_THREAD = 1000;
    private static final int REQUEST_PER_INITIAL_THREAD = 1000;
    private static final AtomicInteger successfulCount = new AtomicInteger(0);
    private static final AtomicInteger failedCount = new AtomicInteger(0);
    private static final int PHASE1_THREAD = 32;
    private static final String LOG_FILE = "request_logs.csv";
    public static void main(String[] args) throws InterruptedException, ExecutionException {

        Path logFilePath = Paths.get(LOG_FILE);
        try {
            if (Files.exists(logFilePath)) {
                Files.delete(logFilePath);
                System.out.println("Deleted " + logFilePath);
            }
        } catch (IOException e) {
            System.out.println("Error deleting csv file 'request_logs.csv': " + e.getMessage());
        }

        Thread eventProducerThread = new Thread(new EventProducer());
        eventProducerThread.start();

        ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
        List<Future<Void>> futures = new ArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        long startTime = System.currentTimeMillis();
        System.out.println("Starting 32 threads...");
        for (int i = 0; i < PHASE1_THREAD ; i++) {
            HTTPClientThread clientThread = new HTTPClientThread(SERVER_URL, successfulCount, failedCount, REQUEST_PER_INITIAL_THREAD, countDownLatch);
            futures.add(executor.submit(clientThread));
        }
        countDownLatch.await();
        System.out.println("Finished 1 of 32 threads!");
        int remainingRequests = TOTAL_REQUESTS - (PHASE1_THREAD * REQUEST_PER_INITIAL_THREAD);
        int threadNeeded = remainingRequests / REQUEST_PER_THREAD;

        System.out.println("Starting additional threads...");

        for (int i = 0; i < threadNeeded; i++) {
            HTTPClientThread clientThread = new HTTPClientThread(SERVER_URL, successfulCount, failedCount, REQUEST_PER_THREAD, null);
            futures.add(executor.submit(clientThread));
        }

        for (Future<Void> future : futures) {
            future.get();
        }

        eventProducerThread.join();
        executor.shutdown();

        long endTime = System.currentTimeMillis();
        long responseTime = endTime - startTime;

        System.out.println("======= Client 1 Output ======= ");
        System.out.println("Number of Threads: " +( threadNeeded + PHASE1_THREAD));
        System.out.println("Successful requests: " + successfulCount.get());
        System.out.println("Failed requests: " + failedCount.get());
        System.out.println("Total requests sent: " + successfulCount.get());

        System.out.println("Total response time: " + responseTime + " ms");
        System.out.println("Throughput: " + (TOTAL_REQUESTS / (responseTime / 1000.0)) + " requests per second");

    }
}


