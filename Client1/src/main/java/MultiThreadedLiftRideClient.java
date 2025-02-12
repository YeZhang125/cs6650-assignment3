import java.net.URI;
import java.net.http.*;
import java.net.http.HttpResponse.BodyHandlers;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Random;

public class MultiThreadedLiftRideClient {
  private static final String SERVER_URL = "http://44.245.81.86:8080/assignment1_war";

  private static final int TOTAL_REQUESTS = 200000;
  private static final int NUM_THREADS =200;
  private static final int BATCH_SIZE = 1000;
   private static final AtomicInteger successfulRequests = new AtomicInteger(0);
  private static final AtomicInteger failedRequests = new AtomicInteger(0);


  public static void main(String[] args) throws InterruptedException {

    // Start event generator
    Thread producerThread = new Thread(new EventProducer());
    producerThread.start();

    // Thread pool for sending requests
    ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
    List<Future<Void>> futures = new ArrayList<>();

    long startTime = System.currentTimeMillis(); // Track wall time

    for (int i = 0; i < NUM_THREADS; i++) {
      HTTPClientThread client = new HTTPClientThread(SERVER_URL, successfulRequests, failedRequests, BATCH_SIZE);
      futures.add(executor.submit(client));
    }

    // Wait for all threads to complete
    for (Future<Void> future : futures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        System.err.println("Thread execution failed: " + e.getMessage());
      }
    }

    // Wait for the producer thread to finish
    try {
      producerThread.join();
    } catch (InterruptedException e) {
      System.err.println("Producer thread interrupted: " + e.getMessage());
    }

    // Shutdown the executor
    executor.shutdown();
    try {
      if (!executor.awaitTermination(10, TimeUnit.MINUTES)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
    }

    // Calculate and print results
    long endTime = System.currentTimeMillis(); // End timing
    double totalTimeSeconds = (endTime - startTime) / 1000.0;
    double throughput = successfulRequests.get() / totalTimeSeconds;
  //  System.out.println("Total requests: " + successfulRequests.get() + failedRequests.get());
    System.out.println("Successful requests: " + successfulRequests.get());
    System.out.println("Failed requests: " + failedRequests.get());
    System.out.println("Total time: " + totalTimeSeconds + " seconds");
    System.out.println("Throughput: " + throughput + " requests/sec");
  }

//  // Producer Thread: Generates Events
//  static class EventProducer implements Runnable {
//
//
//
//  }

//  // Consumer Thread: Sends POST Requests
//  static class PostWorker implements Runnable {
//    private final int batchSize;
//
//    public PostWorker(int batchSize) {
//      this.batchSize = batchSize;
//    }

//    @Override
//    public void run() {
//      for (int i = 0; i < batchSize; i++) {
//        try {
//          String event = eventQueue.take();
//          if (event == null) break; // Exit if no event is available
//
//          boolean success = sendPostRequest(event);
//          if (success) {
//            successfulRequests.incrementAndGet();
//          } else {
//            failedRequests.incrementAndGet();
//          }
//          remainingRequests.decrementAndGet();
//        } catch (InterruptedException e) {
//          Thread.currentThread().interrupt();
//        }
//      }
//    }

//    private boolean sendPostRequest(String event) {
//      HttpRequest request = HttpRequest.newBuilder()
//              .uri(URI.create(SERVER_URL))
//              .POST(HttpRequest.BodyPublishers.ofString(event))
//              .header("Content-Type", "application/json").build();
//
//      int attempts = 0;
//      while (attempts < MAX_RETRIES) {
//        try {
//          HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
//          if (response.statusCode() == 201) {
//            return true; // Successful request
//          } else {
//            System.err.println("Error " + response.statusCode() + ": " + response.body());
//            attempts++;
//            Thread.sleep(1000 * attempts); // Exponential backoff
//          }
//        } catch (IOException | InterruptedException e) {
//          System.err.println("Request failed: Retrying (" + (attempts + 1) + "/" + MAX_RETRIES + ")");
//          attempts++;
//          try {
//            Thread.sleep(1000 * attempts); // Exponential backoff
//          } catch (InterruptedException ex) {
//            Thread.currentThread().interrupt();
//          }
//        }
//      }
//      return false; // Request failed after retries
//    }
//  }
}