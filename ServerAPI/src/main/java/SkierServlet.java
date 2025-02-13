import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.ObjectInputFilter;
import java.io.PrintWriter;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.json.JSONObject;
import org.json.JSONException;

@WebServlet(name = "server.SkierServlet", urlPatterns = "/skiers/*")
public class SkierServlet extends HttpServlet {

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse resp) throws IOException {
    resp.setContentType("application/json");
    Gson gson = new Gson();
    if (request.getPathInfo() != null) {
      String[] url = request.getPathInfo().split("/");
      if (url.length != 8) {
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        resp.getWriter().write(gson.toJson(new ErrorResponse("Invalid URL format! Expected URL: /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}")));        return;
      }

      int resortID = Integer.parseInt(url[1]);
       String dayID = String.valueOf((url[5]));
       int skierID = Integer.parseInt(url[7]);
      if (!dayID.matches("^([1-9]|[1-9][0-9]|[12][0-9][0-9]|3[0-5][0-9]|36[0-6])$")){
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        resp.getWriter().write(gson.toJson(new ErrorResponse("Invalid day ID!")));
        return;
      }

      if (skierID < 1 || skierID > 100000) {
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      resp.getWriter().write(gson.toJson(new ErrorResponse("Invalid skierID!")));
        return;
      }


      if (resortID < 1 || resortID>10) {
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        resp.getWriter().write(gson.toJson(new ErrorResponse("Invalid resortID!")));
        return;
      }
      StringBuilder sb = new StringBuilder();
      try (BufferedReader reader = request.getReader()) {
        String line;
        while ((line = reader.readLine()) != null) {
          sb.append(line);
        }
      }
      JsonObject jsonObject = gson.fromJson(sb.toString(), JsonObject.class);

      if(!jsonObject.has("liftID" )) {
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        resp.getWriter().write(gson.toJson(new ErrorResponse("Missing required fields liftID")));
        return;
      }


      if(!jsonObject.has("time" )) {
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        resp.getWriter().write(gson.toJson(new ErrorResponse("Missing required fields time")));
        return;
      }
    }

    resp.setStatus(HttpServletResponse.SC_CREATED);
    resp.getWriter().write(gson.toJson(new SuccessResponse("Skier processed successfully")));
    }
  static class ErrorResponse {
    String message;
    ErrorResponse(String message) { this.message = message; }
  }
  static class SuccessResponse {
    String message;
    SuccessResponse(String message) { this.message = message; }
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
          throws  IOException {

    response.setContentType("text/html");
    PrintWriter out = response.getWriter();
    out.println("<h1>It works!:)</h1>");
  }
}

