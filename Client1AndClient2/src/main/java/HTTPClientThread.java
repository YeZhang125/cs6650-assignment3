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

    private static final HttpClient client = HttpClient.newHttpClient();
    private static final String LOG_FILE = "request_logs.csv";

    private final String baseUrl;
    private final AtomicInteger successfulRequests;
    private final AtomicInteger failedRequests;
    private final Integer requestPerThread;

    public HTTPClientThread(String baseUrl, AtomicInteger successfulRequests, AtomicInteger failedRequests, Integer requestPerThread) {
        this.baseUrl = baseUrl;
        this.successfulRequests = successfulRequests;
        this.failedRequests = failedRequests;
        this.requestPerThread = requestPerThread;
    }

    @Override
    public Void call() {
        try (PrintWriter writer = new PrintWriter(new FileWriter(LOG_FILE, true))) {
            for (int i = 0; i < requestPerThread; i++) {
                SkierLiftEvent event = EventProducer.getEvent();

                String eventUrl = String.format(
                        "%s/skiers/%d/seasons/%d/days/%d/skiers/%d",
                        baseUrl, event.getResortID(), event.getSeasonID(), event.getDayID(), event.getSkierID()
                );

                Gson gson = new Gson();
                String json = gson.toJson(event);

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(eventUrl))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(json))
                        .build();

                int retries = 0;
                while (retries < 5) {
                    long startTime = System.currentTimeMillis();
                    try {
                        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                        long endTime = System.currentTimeMillis();
                        long latency = endTime - startTime;
                        double throughput = 1000.0 / latency;

                      logRequestData(writer, startTime, latency, response.statusCode(), throughput);

                        if ( response!=null && response.statusCode() == 201) {
                          //  logRequestData(writer, startTime, latency, response.statusCode(), throughput);
                            successfulRequests.incrementAndGet();

                            break;  // Success, exit retry loop
                        } else {
                             System.err.println("Request failed: " + response.statusCode() + " - " + response.body());
                            retries++;
                            backoffDelay(retries);
                        }
                    } catch (Exception e) {
                        long endTime = System.currentTimeMillis();
                        long latency = endTime - startTime;
                        double throughput = 1000.0 / latency;

                        logRequestData(writer, startTime, latency, 500, throughput);
                        System.err.println("Exception on request: " + e.getMessage());

                        retries++;
                        if (retries < 5) {
                            backoffDelay(retries);
                        } else {
                            failedRequests.incrementAndGet();
                        }
                    }
                }
                //  Throttle requests slightly to reduce load
                TimeUnit.MILLISECONDS.sleep(100);
            }
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    private synchronized void logRequestData(PrintWriter writer, long startTime, long latency, int statusCode, double throughput) {
        writer.printf("%d,POST,%d,%d,%.2f%n", startTime, latency, statusCode, throughput);
        writer.flush();
    }

    private void backoffDelay(int retries) throws InterruptedException {
        int delay = (int) Math.pow(2, retries) * 100; // Exponential backoff (100ms, 200ms, 400ms...)
        TimeUnit.MILLISECONDS.sleep(delay);
    }
}
