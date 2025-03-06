
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


public class MultiThreadedLiftRideClient {
   private static final String SERVER_URL = "http://52.37.115.176:8080/assignment1_war";
  private static final int TOTAL_REQUESTS = 200000;
  private static final int NUMBER_OF_THREADS = 500;
  private static final int REQUEST_PER_THREAD = 400;
  private static final AtomicInteger successfulCount = new AtomicInteger(0);
  private static final AtomicInteger failedCount = new AtomicInteger(0);
  private static final  String LOG_FILE = "request_logs.csv";
  public static void main(String[] args) throws InterruptedException, ExecutionException {
    Thread eventProducerThread = new Thread(new EventProducer());
    eventProducerThread.start();

    ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
    List<Future<Void>> futures = new ArrayList<>();

    long startTime = System.currentTimeMillis();

    for (int i = 0; i < NUMBER_OF_THREADS; i++) {
      HTTPClientThread clientThread = new HTTPClientThread(SERVER_URL, successfulCount, failedCount, REQUEST_PER_THREAD);
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
    System.out.println("Number of Threads: " + NUMBER_OF_THREADS);
    System.out.println("Successful requests: " + successfulCount.get());
    System.out.println("Failed requests: " + failedCount.get());
    System.out.println("Total requests sent: " + successfulCount.get());

    System.out.println("Total response time: " + responseTime + " ms");
    System.out.println("Throughput: " + (TOTAL_REQUESTS / (responseTime / 1000.0)) + " requests per second");


   // LatencyComputationForClient2.latencyComputation(LOG_FILE, TOTAL_REQUESTS);
  }
}