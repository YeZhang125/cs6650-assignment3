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
import java.util.UUID;
import java.util.concurrent.TimeoutException;

@WebServlet(name = "SkierServlet", urlPatterns = "/skiers/*")
public class SkierServlet extends HttpServlet {
  private static final String USERNAME = "test_user";
  private static final String PASSWORD = "test_password";
  private static final String HOST = "54.203.65.33";
   private RabbitMQClient rabbitMQClient;
  private Gson gson = new Gson();

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
    response.setContentType("text/html");
    response.setStatus(HttpServletResponse.SC_OK);
    PrintWriter out = response.getWriter();
    out.println("<h1>It works! :)</h1>");
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
}
