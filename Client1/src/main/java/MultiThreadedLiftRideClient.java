
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


public class MultiThreadedLiftRideClient {
   private static final String SERVER_URL = "http://18.246.232.176:8080/assignment1_war";
  private static final int TOTAL_REQUESTS = 200000;
  private static final int NUMBER_OF_THREADS = 200;
  private static final int REQUEST_PER_THREAD = 1000;
  private static final AtomicInteger successfulRequests = new AtomicInteger(0);
  private static final AtomicInteger failedRequests = new AtomicInteger(0);

  public static void main(String[] args) throws InterruptedException, ExecutionException {
    Thread eventProducerThread = new Thread(new EventProducer());
    eventProducerThread.start();

    ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
    List<Future<Void>> futures = new ArrayList<>();

    long startTime = System.currentTimeMillis();

    for (int i = 0; i < NUMBER_OF_THREADS; i++) {
      HTTPClientThread clientThread = new HTTPClientThread(SERVER_URL, successfulRequests, failedRequests, REQUEST_PER_THREAD);
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
    System.out.println("Successful requests: " + successfulRequests.get());
    System.out.println("Failed requests: " + failedRequests.get());
    System.out.println("Total requests sent: " + successfulRequests.get());

    System.out.println("Total response time: " + responseTime + " ms");
    System.out.println("Throughput: " + (TOTAL_REQUESTS / (responseTime / 1000.0)) + " requests per second");

    LatencyComputationForClient2.latencyComputation("request_logs.csv", TOTAL_REQUESTS);
  }
}