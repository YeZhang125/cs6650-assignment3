
import com.google.gson.Gson;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class HTTPClientThread implements Callable<Void> {

    // HTTP Client for making requests
    private static final HttpClient client = HttpClient.newHttpClient();

    private final String baseUrl;
    private final AtomicInteger successfulRequests;
    private final AtomicInteger failedRequests;
    private final Integer requestPerThread;
    private static final String LOG_FILE = "request_logs.csv";

    // Constructor to initialize the thread parameters
    public HTTPClientThread(String baseUrl, AtomicInteger successfulRequests, AtomicInteger failedRequests, Integer requestPerThread) {
        this.baseUrl = baseUrl;
        this.successfulRequests = successfulRequests;
        this.failedRequests = failedRequests;
        this.requestPerThread = requestPerThread;
    }

    @Override
    public Void call() {
        try (PrintWriter writer = new PrintWriter(new FileWriter(LOG_FILE, true))) {
            // Loop through the number of requests each thread is supposed to make
            for (int i = 0; i < requestPerThread; i++) {
                // Generate a new SkierLiftRideEvent (replace with actual event generation method)
                SkierLiftEvent event = EventProducer.getEvent();

                // Build the event URL dynamically based on the event data
                String eventUrl = String.format(
                        "%s/skiers/%d/seasons/%d/days/%d/skiers/%d",
                        baseUrl, event.getResortID(), event.getSeasonID(), event.getDayID(), event.getSkierID()
                );

                // Convert the event object into a JSON string
                Gson gson = new Gson();
                String json = gson.toJson(event);

                // Create the HTTP POST request
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(eventUrl))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(json))
                        .build();

                int retries = 0;
                while (retries < 5) {
                    long startTime = System.currentTimeMillis();
                    try {
                        // Send the request and capture the response
                        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                        long endTime = System.currentTimeMillis();
                        long latency = endTime - startTime;
                        double throughput = 1000.0 / latency;

                        // Log the request details
                        synchronized (HTTPClientThread.class) {
                            writer.printf("%d,POST,%d,%d,%.2f%n", startTime, latency, response.statusCode(), throughput);
                            writer.flush();
                        }

                        // Check if the response was successful (201 - Created)
                        if (response.statusCode() == 201) {
                            successfulRequests.incrementAndGet();
                            break;
                        } else {
                           // failedRequests.incrementAndGet();
                            retries++;
                            if (retries >= 5) {
                                failedRequests.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        // If an exception occurs, log the error and retry
                        long endTime = System.currentTimeMillis();
                        long latency = endTime - startTime;
                        double throughput = 1000.0 / latency;

                        synchronized (HTTPClientThread.class) {
                            writer.printf("%d,POST,%d,500,%.2f%n", startTime, latency, throughput);
                            writer.flush();
                        }

                        //failedRequests.incrementAndGet();
                        retries++;
                        if (retries >= 5) {
                            failedRequests.incrementAndGet();
                        }

                        // Retry after a short delay if not successful
                        if (retries < 5) {
                            TimeUnit.SECONDS.sleep(1);
                        }

                    }
                }
            }
        } catch (IOException | InterruptedException e) {
            // Handle exceptions (e.g., file IO or thread interruption)
            throw new RuntimeException(e);
        }
        return null;
    }
}
