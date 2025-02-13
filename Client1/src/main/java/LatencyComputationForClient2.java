

import java.io.BufferedReader;

import java.io.FileReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LatencyComputationForClient2 {
     private static final int INITIAL_THREADS = 200;
  public static void latencyComputation(String logFile, int totalRequests) {
    List<Long> latencies = new ArrayList<>();
    long totalLatency = 0;
    long minLatency = Long.MAX_VALUE;
    long maxLatency = Long.MIN_VALUE;

    try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] parts = line.split(",");
        if (parts.length < 4) continue;

        long latency = Long.parseLong(parts[2]);
        latencies.add(latency);

        minLatency = Math.min(minLatency, latency);
        maxLatency = Math.max(maxLatency, latency);
        totalLatency += latency;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    if (latencies.isEmpty()) {
      System.out.println("No latency data found!");
      return;
    }

    Collections.sort(latencies);
    double meanLatency = totalLatency / (double) totalRequests;
    long medianLatency = latencies.get(latencies.size() / 2);
    long p99Latency = latencies.get((int) (latencies.size() * 0.99));
    double throughput = totalRequests / (totalLatency / 1000.0);

    System.out.println();
    System.out.println("This is the Part 2 Output: ");
    System.out.println("Mean Response Time: " + meanLatency + " ms");
    System.out.println("Median Response Time: " + medianLatency + " ms");
    System.out.println("99th Percentile Response Time: " + p99Latency + " ms");
    System.out.println("Min Response Time: " + minLatency + " ms");
    System.out.println("Max Response Time: " + maxLatency + " ms");
    System.out.println("Throughput per thread: " + throughput + " requests/sec");
    System.out.println("Estimated throughput for " + INITIAL_THREADS + " thread: " + throughput * INITIAL_THREADS + " requests/sec");
  }
}



