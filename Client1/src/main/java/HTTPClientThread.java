import com.google.gson.Gson;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Random;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class HTTPClientThread implements Callable {
    private static final HttpClient client = HttpClient.newHttpClient();
    private final String baseUrl;
    private final AtomicInteger successfulRequests;
    private final AtomicInteger failedRequests;
    private final Integer requestPerThread;
    private static final  int RETRIES =5;
    private final String LOG_FILE = "request_logs.csv";

    public HTTPClientThread(String baseUrl, AtomicInteger successfulRequests, AtomicInteger failedRequests, Integer requestPerThread) throws InterruptedException {
        this.baseUrl = baseUrl;
        this.successfulRequests = successfulRequests;
        this.failedRequests = failedRequests;
        this.requestPerThread = requestPerThread;
    }


    @Override
    public Object call() throws InterruptedException, IOException {
        SkierLiftEvent e = EventProducer.getEvent();
        String url = null;
        for (int i = 0; i < requestPerThread; i++) {
            System.out.println("Thread " + Thread.currentThread().getName() + " is processing task " + (i + 1));
            url = String.format("%s/skiers/%d/seasons/%d/days/%d/time/%d/skier/%d",
                    baseUrl, e.getResortID(), e.getSeasonID(), e.getDayID(), e.getTime(), e.getSkierID());
            Gson gson = new Gson();
            String json = gson.toJson(e);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .build();

            int retries = 0;
            while (retries < RETRIES) {
                long startTime = System.currentTimeMillis();
                try {
                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    long endTime = System.currentTimeMillis();
                    long latency = endTime - startTime;
                    double throughput = 1000.0 / latency;
                    writeToCSV(startTime, latency, response.statusCode(), throughput, response);
                    if (response.statusCode() == 201) {
                        successfulRequests.incrementAndGet();
                        break;
                    } else {
                        failedRequests.incrementAndGet();
                        retries++;
                    }
                } catch (InterruptedException exception) {
                    long endTime = System.currentTimeMillis();
                    long latency = endTime - startTime;
                    double throughput = 1000.0 / latency;
                    writeToCSVExeption(startTime, latency, throughput);
                    failedRequests.incrementAndGet();
                    retries++;
                    if (retries >= 5) {
                        break;
                    }
                    TimeUnit.SECONDS.sleep(1);
                }
            }

        }
        return null;
    }

    private void writeToCSV(long startTime, long latency, int i, double throughput,  HttpResponse<String> response) throws IOException {
        PrintWriter writer = new PrintWriter(new FileWriter(LOG_FILE, true) );

        synchronized (HTTPClientThread.class) {
            writer.printf("%d,POST,%d,%d,%.2f%n", startTime, latency, response.statusCode(), throughput);
            writer.flush();
        }


    }

    private void writeToCSVExeption(long startTime, long latency,  double throughput ) throws IOException {
        PrintWriter writer = new PrintWriter(new FileWriter(LOG_FILE, true) );

        synchronized (HTTPClientThread.class) {
            writer.printf("%d,POST,%d,500,%.2f%n", startTime, latency, throughput);
            writer.flush();
        }


    }
}





