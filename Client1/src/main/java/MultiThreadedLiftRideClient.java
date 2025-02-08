import java.net.URI;
import java.net.http.*;
import java.net.http.HttpResponse.BodyHandlers;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Random;

public class MultiThreadedLiftRideClient {
  private static final int TOTAL_REQUESTS = 200000;
  private static final int INITIAL_THREADS = 32;
  private static final int INITIAL_BATCH_SIZE = 1000;
  private static final int MAX_THREADS = 100;
  private static final int MAX_RETRIES = 5;
  private static final String SERVER_URL ="http://52.88.171.176:8080/assignment1_war/skiers";


  private static final BlockingQueue<String> eventQueue = new LinkedBlockingQueue<>(5000);
  private static final AtomicInteger successfulRequests = new AtomicInteger(0);
  private static final AtomicInteger failedRequests = new AtomicInteger(0);
  private static final AtomicInteger remainingRequests = new AtomicInteger(TOTAL_REQUESTS);

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

    System.out.println("Successful requests: " + successfulRequests.get());
    System.out.println("Failed requests: " + failedRequests.get());
    System.out.println("Total time: " + totalTimeSeconds + " seconds");
    System.out.println("Throughput: " + throughput + " requests/sec");
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
          HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
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
  }
}
