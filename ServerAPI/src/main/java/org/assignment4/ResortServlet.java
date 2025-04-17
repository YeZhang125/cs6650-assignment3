package org.assignment4;

import com.google.gson.Gson;
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
    private static final String REDIS_HOST = "34.220.88.62";
    private static final int REDIS_PORT = 6379;
    private static final JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), REDIS_HOST, REDIS_PORT);
    private Gson gson = new Gson();

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

    /**
     * Handles the GET request for /resorts/{resortID}/seasons/{seasonID}/day/{dayID}/skiers
     * Returns the number of unique skiers at the specified resort on the specified day
     */
    private void handleResortDaySkiersRequest(HttpServletRequest request, HttpServletResponse response, String[] pathParts) throws IOException {
        PrintWriter out = response.getWriter();

        try {
            // Parse and validate resortID (between 1 and 10)
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
            long uniqueSkiers = 0;
            try (Jedis jedis = jedisPool.getResource()) {
                // The key format matches what we have in SkierConsumer.java
                String resortKey = "resort:" + resortID + ":season:" + seasonID + ":day:" + dayID + ":skiers";
                uniqueSkiers = jedis.scard(resortKey); // Get the count of unique skiers using SCARD
            }

            // Create and send the response
            response.setStatus(HttpServletResponse.SC_OK);
            SkierCount skierCount = new SkierCount((int) uniqueSkiers);
            out.print(gson.toJson(skierCount));
        } catch (Exception e) {
            System.err.println("Error retrieving unique skiers count: " + e.getMessage());
            sendErrorResponse(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    "Error retrieving unique skiers count: " + e.getMessage());
        }
    }

    /**
     * Sends an error response with the specified status code and message
     */
    private void sendErrorResponse(HttpServletResponse response, int statusCode, String message) throws IOException {
        response.setStatus(statusCode);
        PrintWriter out = response.getWriter();
        ErrorResponse errorMsg = new ErrorResponse(message);
        out.print(gson.toJson(errorMsg));
    }

    /**
     * Error response class for consistent error formatting
     */
    static class ErrorResponse {
        String message;
        ErrorResponse(String message) { this.message = message; }
    }

    /**
     * Response class for the unique skier count
     */
    static class SkierCount {
        int uniqueSkiers;
        SkierCount(int uniqueSkiers) { this.uniqueSkiers = uniqueSkiers; }
    }
}