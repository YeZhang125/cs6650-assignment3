import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;


@WebServlet(name = "SkierServlet", urlPatterns = "/skiers/*")
public class SkierServlet extends HttpServlet {
  //  private static final String USERNAME = "test_user";
  private static final String USERNAME = "admin";
  //  private static final String PASSWORD = "test_password";
  private static final String PASSWORD = "admin";
  //  private static final String HOST = "54.203.65.33";
  private static final String HOST = "34.220.128.228";
  private RabbitMQClient rabbitMQClient;
  private Gson gson = new Gson();

  private static final String REDIS_HOST = "54.71.181.20";
  private static final int REDIS_PORT = 6379;
  private static final JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), REDIS_HOST , REDIS_PORT);

  @Override
  public void init() throws ServletException {
    try {
      rabbitMQClient = new RabbitMQClient(HOST, USERNAME, PASSWORD);
    } catch (IOException | TimeoutException e) {
      throw new ServletException("Failed to initialize RabbitMQ client", e);
    }
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse resp) throws IOException {
    resp.setContentType("application/json");

    if (request.getPathInfo() == null) {
      sendErrorResponse(resp, "Invalid URL format!");
      return;
    }

    String[] url = request.getPathInfo().split("/");
    if (url.length != 8) {
      sendErrorResponse(resp, "Invalid URL format! Expected: /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}");
      return;
    }

    String dayID = url[5];
    String seasonID = url[3];
    int resortID = parseInt(url[1], resp, "Invalid resortID!");
    int skierID = parseInt(url[7], resp, "Invalid skierID!");

    if (!seasonID.matches("\\d+") || !dayID.matches("\\d+")) {
      sendErrorResponse(resp, "Either seasonID or dayID is not a positive integer.");
      return;
    }

    if (!dayID.matches("^([1-9]|[1-9][0-9]|[12][0-9][0-9]|3[0-5][0-9]|36[0-6])$")) {
      sendErrorResponse(resp, "Invalid day ID!");
      return;
    }

    if (skierID < 1 || skierID > 100000 || resortID < 1 || resortID > 10) {
      sendErrorResponse(resp, "Invalid skierID or resortID!");
      return;
    }

    JsonObject jsonObject = parseAndValidateJson(request, resp);
    if (jsonObject == null) return;
    if (!jsonObject.has("liftID") || !jsonObject.has("time")) {
      sendErrorResponse(resp, "Missing required fields (liftID, time)");
      return;
    }
    // Add correlation ID to the JSON message
    String correlationID = UUID.randomUUID().toString();
    jsonObject.addProperty("correlationID", correlationID);
    jsonObject.addProperty("resortID", resortID);
    jsonObject.addProperty("dayID", dayID);
    jsonObject.addProperty("skierID", skierID);
    jsonObject.addProperty("seasonID", skierID);
    jsonObject.addProperty("time", jsonObject.get("time").getAsString());
    jsonObject.addProperty("liftID", jsonObject.get("liftID").getAsInt());

    try {
      rabbitMQClient.publishMessage(jsonObject.toString());
      sendSuccessResponse(resp);
    } catch (Exception e) {
      sendErrorResponse(resp, "Failed to send message to RabbitMQ: " + e.getMessage());
    }

  }

  @Override
  public void destroy() {
    try {
      if (rabbitMQClient != null) {
        rabbitMQClient.close();
      }
    } catch (IOException | TimeoutException e) {
      System.out.println("Failed to close RabbitMQ connection");
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

      // Check which API endpoint is being accessed
      if (pathParts.length == 3 && pathParts[2].equals("vertical")) {
        // Handle /skiers/{skierID}/vertical endpoint
        handleSkierVerticalRequest(request, response, pathParts);
      } else if (pathParts.length == 8 && pathParts[2].equals("seasons")) {
        // Handle /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID} endpoint
        handleSkierDayVerticalRequest(request, response, pathParts);
      } else {
        // Simple response for other paths
        response.setStatus(HttpServletResponse.SC_OK);
        out.println("<h1>It works! :)</h1>");
      }
    } catch (Exception e) {
      System.err.println("Error processing request: " + e.getMessage());
      sendErrorResponse(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
              "An unexpected error occurred: " + e.getMessage());
    }
  }

  // Handle /skiers/{skierID}/vertical endpoint
  // Method to handle the endpoint for skier vertical by resort
  private void handleSkierVerticalRequest(HttpServletRequest request, HttpServletResponse response, String[] pathParts) throws IOException {
    PrintWriter out = response.getWriter();

    try {
      // Parse and validate skierID (between 1 and 100000)
      int skierID;
      try {
        skierID = Integer.parseInt(pathParts[1]);
        if (skierID < 1 || skierID > 100000) {
          sendErrorResponse(response, HttpServletResponse.SC_BAD_REQUEST,
                  "Invalid skierID. Must be between 1 and 100000.");
          return;
        }
      } catch (NumberFormatException e) {
        sendErrorResponse(response, HttpServletResponse.SC_BAD_REQUEST,
                "Invalid skierID format. Must be an integer.");
        return;
      }

      // Get query parameters
      String resortParam = request.getParameter("resort");
      String seasonParam = request.getParameter("season");

      if (resortParam == null || resortParam.trim().isEmpty()) {
        sendErrorResponse(response, HttpServletResponse.SC_BAD_REQUEST,
                "Resort parameter is required");
        return;
      }

      // Validate resortID (between 1 and 10)
      int resortID;
      try {
        resortID = Integer.parseInt(resortParam);
        if (resortID < 1 || resortID > 10) {
          sendErrorResponse(response, HttpServletResponse.SC_BAD_REQUEST,
                  "Invalid resort ID. Must be between 1 and 10.");
          return;
        }
      } catch (NumberFormatException e) {
        sendErrorResponse(response, HttpServletResponse.SC_BAD_REQUEST,
                "Resort must be provided as a resort ID (integer between 1 and 10)");
        return;
      }

      // Query Redis for vertical data
      try (Jedis jedis = jedisPool.getResource()) {
        Map<String, String> verticalData;
        int totalVertical = 0;

        if (seasonParam != null && !seasonParam.trim().isEmpty()) {
          // Validate seasonID (should be 2025)
          if (!seasonParam.equals("2025")) {
            sendErrorResponse(response, HttpServletResponse.SC_BAD_REQUEST,
                    "Invalid season ID. Must be 2025.");
            return;
          }

          // Get vertical for specific resort and season
          String resortSeasonField = "resort:" + resortID + ":season:" + seasonParam;
          String totalByResortSeasonKey = "skier:" + skierID + ":vertical:byResortSeason";
          String verticalValue = jedis.hget(totalByResortSeasonKey, resortSeasonField);

          if (verticalValue != null) {
            totalVertical = Integer.parseInt(verticalValue);
          }
        } else {
          // If no season specified, get total vertical for the resort across all seasons
          String resortField = "resort:" + resortID;
          String totalByResortKey = "skier:" + skierID + ":vertical:byResort";
          String verticalValue = jedis.hget(totalByResortKey, resortField);

          if (verticalValue != null) {
            totalVertical = Integer.parseInt(verticalValue);
          }
        }

        // Create response object
        SkierVertical skierVertical = new SkierVertical();
        ResortVertical resortVertical = new ResortVertical();
        resortVertical.setResortID(resortID);
        resortVertical.setTotalVert(totalVertical);

        List<ResortVertical> resortVerticals = new ArrayList<>();
        resortVerticals.add(resortVertical);
        skierVertical.setResorts(resortVerticals);

        // Send response
        response.setStatus(HttpServletResponse.SC_OK);
        out.print(gson.toJson(skierVertical));
      }
    } catch (Exception e) {
      System.err.println("Error retrieving vertical data: " + e.getMessage());
      sendErrorResponse(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
              "Error retrieving vertical data: " + e.getMessage());
    }
  }


  // Handle /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID} endpoint
  // Method to handle the endpoint for skier day vertical
  private void handleSkierDayVerticalRequest(HttpServletRequest request, HttpServletResponse response, String[] pathParts) throws IOException {
    PrintWriter out = response.getWriter();

    try {
      // Validate URL format
      if (pathParts.length != 8) {
        sendErrorResponse(response, HttpServletResponse.SC_BAD_REQUEST,
                "Invalid URL format. Expected: /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}");
        return;
      }

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

      // Parse and validate skierID (between 1 and 100000)
      int skierID;
      try {
        skierID = Integer.parseInt(pathParts[7]);
        if (skierID < 1 || skierID > 100000) {
          sendErrorResponse(response, HttpServletResponse.SC_BAD_REQUEST,
                  "Invalid skierID. Must be between 1 and 100000.");
          return;
        }
      } catch (NumberFormatException e) {
        sendErrorResponse(response, HttpServletResponse.SC_BAD_REQUEST,
                "Invalid skierID format. Must be an integer.");
        return;
      }

      // Query Redis for vertical data
      int totalVertical = 0;

      try (Jedis jedis = jedisPool.getResource()) {
        // Method 1: Try to get detailed vertical data (if implemented)
        String detailedVerticalKey = "skier:" + skierID + ":vertical:detailed";
        String detailedField = "resort:" + resortID + ":season:" + seasonID + ":day:" + dayID;
        String detailedValue = jedis.hget(detailedVerticalKey, detailedField);

        if (detailedValue != null) {
          totalVertical = Integer.parseInt(detailedValue);
        } else {
          // Method 2: Calculate from all lift rides for the day
          String skierDayKey = "skier:" + skierID + ":" + seasonID + ":" + dayID;
          Map<String, String> liftRides = jedis.hgetAll(skierDayKey);

          // Sum vertical for each lift ride (assuming 10 vertical units per lift ID)
          for (String liftEntry : liftRides.keySet()) {
            if (liftEntry.startsWith("liftID: ")) {
              int liftID = Integer.parseInt(liftEntry.substring(8).trim());
              // Validate liftID is between 1 and 40
              if (liftID >= 1 && liftID <= 40) {
                totalVertical += liftID * 10;
              }
            }
          }

          // Method 3: Try getting from daily vertical record as fallback
          if (totalVertical == 0) {
            String verticalKey = "skier:" + skierID + ":vertical";
            String field = "day:" + dayID;
            String verticalValue = jedis.hget(verticalKey, field);

            if (verticalValue != null) {
              totalVertical = Integer.parseInt(verticalValue);
            }
          }
        }

        // Send response
        response.setStatus(HttpServletResponse.SC_OK);
        out.print(totalVertical);
      }
    } catch (Exception e) {
      System.err.println("Error retrieving vertical data: " + e.getMessage());
      sendErrorResponse(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
              "Error retrieving vertical data: " + e.getMessage());
    }
  }


  private int parseInt(String value, HttpServletResponse resp, String errorMsg) throws IOException {
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      sendErrorResponse(resp, errorMsg);
      return -1;
    }
  }

  private String readRequestBody(HttpServletRequest request) throws IOException {
    StringBuilder sb = new StringBuilder();
    try (BufferedReader reader = request.getReader()) {
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }
    }
    return sb.toString();
  }

  private JsonObject parseAndValidateJson(HttpServletRequest request, HttpServletResponse response) throws IOException {
    String requestBody = readRequestBody(request);
    try {
      return gson.fromJson(requestBody, JsonObject.class);
    } catch (JsonSyntaxException e) {
      sendErrorResponse(response, "Invalid JSON syntax.");
      return null;
    }
  }

  private void sendErrorResponse(HttpServletResponse resp, String message) throws IOException {
    resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    resp.getWriter().write(gson.toJson(new ErrorResponse(message)));
  }

  private void sendErrorResponse(HttpServletResponse response, int statusCode, String message) throws IOException {
    response.setStatus(statusCode);
    PrintWriter out = response.getWriter();
    ErrorResponse errorMsg = new ErrorResponse(message);
    out.print(gson.toJson(errorMsg));
  }

  private void sendSuccessResponse(HttpServletResponse resp) throws IOException {
    resp.setStatus(HttpServletResponse.SC_CREATED);
    resp.getWriter().write(gson.toJson(new SuccessResponse("Skier processed successfully in queue: skier_queue_1")));
  }


  static class ErrorResponse {
    String message;
    ErrorResponse(String message) { this.message = message; }
  }

  static class SuccessResponse {
    String message;
    SuccessResponse(String message) { this.message = message; }
  }

  static class SkierVertical {
    private List<ResortVertical> resorts;

    public List<ResortVertical> getResorts() {
      return resorts;
    }

    public void setResorts(List<ResortVertical> resorts) {
      this.resorts = resorts;
    }
  }

  static class ResortVertical {
    private int resortID;
    private String seasonID; // Optional
    private int totalVert;

    public int getResortID() {
      return resortID;
    }

    public void setResortID(int resortID) {
      this.resortID = resortID;
    }

    public String getSeasonID() {
      return seasonID;
    }

    public void setSeasonID(String seasonID) {
      this.seasonID = seasonID;
    }

    public int getTotalVert() {
      return totalVert;
    }

    public void setTotalVert(int totalVert) {
      this.totalVert = totalVert;
    }
  }
}
