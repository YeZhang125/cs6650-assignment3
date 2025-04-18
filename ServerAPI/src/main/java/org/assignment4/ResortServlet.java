package org.assignment4;

import com.google.gson.Gson;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.servlet.ServletException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Servlet handling resort-related API endpoints
 */
@WebServlet(name = "org.assignment4.ResortServlet", urlPatterns = "/resorts/*")
public class ResortServlet extends HttpServlet {
    // Reuse the same Redis connection info from org.assignment4.SkierServlet
    private static final String REDIS_HOST = "35.94.253.133";
    private static final int REDIS_PORT = 6379;
    private static final JedisPool jedisPool;
    private Gson gson = new Gson();
    private static final ThreadLocal<Jedis> localJedis = new ThreadLocal<>();


    static {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(200);
        poolConfig.setMaxIdle(50);
        poolConfig.setMinIdle(10);
        poolConfig.setTestOnBorrow(false);
        poolConfig.setTestOnReturn(false);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setBlockWhenExhausted(true);

        jedisPool = new JedisPool(poolConfig, REDIS_HOST, REDIS_PORT, 2000);
    }

    @Override
    public void init() throws ServletException {
        super.init();
        warmUpPool();
    }


    private void warmUpPool() {
        try {
            Jedis[] warmupConnections = new Jedis[10];
            for (int i = 0; i < warmupConnections.length; i++) {
                warmupConnections[i] = jedisPool.getResource();
            }
            for (Jedis jedis : warmupConnections) {
                jedis.close();
            }
        } catch (Exception e) {
            getServletContext().log("Redis pool warmup failed: " + e.getMessage());
        }
    }


    private Jedis getJedis() {
        Jedis jedis = localJedis.get();
        if (jedis == null) {
            jedis = jedisPool.getResource();
            localJedis.set(jedis);
        }
        return jedis;
    }

    @Override
    public void destroy() {
        super.destroy();
        Jedis jedis = localJedis.get();
        if (jedis != null) {
            jedis.close();
            localJedis.remove();
        }
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        PrintWriter out = response.getWriter();

        try {
            String pathInfo = request.getPathInfo();
            if (pathInfo == null) {
                sendErrorResponse(response, HttpServletResponse.SC_BAD_REQUEST, "Invalid URL format");
                return;
            }

            String[] pathParts = pathInfo.split("/");

            // Check for the specific endpoint pattern
            // /resorts/{resortID}/seasons/{seasonID}/day/{dayID}/skiers
            if (pathParts.length == 7 &&
                pathParts[2].equals("seasons") &&
                pathParts[4].equals("day") &&
                pathParts[6].equals("skiers")) {

                handleResortDaySkiersRequest(request, response, pathParts);
            } else {
                // Handle other resort-related endpoints or return a "not implemented" response
                sendErrorResponse(response, HttpServletResponse.SC_NOT_IMPLEMENTED,
                    "This endpoint is not implemented");
            }
        } catch (Exception e) {
            System.err.println("Error processing request: " + e.getMessage());
            sendErrorResponse(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                "An unexpected error occurred: " + e.getMessage());
        }
    }

    private void handleResortDaySkiersRequest(HttpServletRequest request, HttpServletResponse response, String[] pathParts) throws IOException {
        PrintWriter out = response.getWriter();

        try {
            // Parse and validate resortID
            int resortID;
            try {
                resortID = Integer.parseInt(pathParts[1]);
                if (resortID < 1 || resortID > 10) {
                    sendErrorResponse(response, HttpServletResponse.SC_BAD_REQUEST,
                        "Invalid resortID. Must be between 1 and 10.");
                    return;
                }
            } catch (NumberFormatException e) {
                sendErrorResponse(response, HttpServletResponse.SC_BAD_REQUEST,
                    "Invalid resortID format. Must be an integer.");
                return;
            }

            // Validate seasonID (should be 2025)
            String seasonID = pathParts[3];
            if (!seasonID.equals("2025")) {
                sendErrorResponse(response, HttpServletResponse.SC_BAD_REQUEST,
                    "Invalid seasonID. Must be 2025.");
                return;
            }

            // Validate dayID (between 1 and 366)
            String dayID = pathParts[5];
            try {
                int day = Integer.parseInt(dayID);
                if (day < 1 || day > 366) {
                    sendErrorResponse(response, HttpServletResponse.SC_BAD_REQUEST,
                        "Invalid dayID. Must be between 1 and 366.");
                    return;
                }
            } catch (NumberFormatException e) {
                sendErrorResponse(response, HttpServletResponse.SC_BAD_REQUEST,
                    "Invalid dayID format. Must be an integer.");
                return;
            }

            // Query Redis for the number of unique skiers
            Jedis jedis = getJedis();
            String resortKey = "resort:" + resortID + ":season:" + seasonID + ":day:" + dayID + ":skiers";
            long uniqueSkiers = jedis.scard(resortKey);


            response.setStatus(HttpServletResponse.SC_OK);
            out.print("{\"uniqueSkiers\":" + uniqueSkiers + "}");
        } catch (Exception e) {
            System.err.println("Error retrieving unique skiers count: " + e.getMessage());
            sendErrorResponse(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                "Error retrieving unique skiers count: " + e.getMessage());
        }
    }

    private static final String BAD_REQUEST_ERROR_PREFIX = "{\"message\":\"";
    private static final String ERROR_SUFFIX = "\"}";

    private void sendErrorResponse(HttpServletResponse response, int statusCode, String message) throws IOException {
        response.setStatus(statusCode);
        PrintWriter out = response.getWriter();
        out.print(BAD_REQUEST_ERROR_PREFIX + message + ERROR_SUFFIX);
    }
}