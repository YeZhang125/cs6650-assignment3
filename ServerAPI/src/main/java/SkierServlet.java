import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import org.json.JSONObject;
import org.json.JSONException;

@WebServlet("/skiers")
public class SkierServlet extends HttpServlet {

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    resp.setContentType("application/json");
    resp.setCharacterEncoding("UTF-8");

    StringBuilder jsonString = new StringBuilder();
    String line;
    while ((line = req.getReader().readLine()) != null) {
      jsonString.append(line);
    }

    try {
      JSONObject json = new JSONObject(jsonString.toString());

      // Validate required fields
      if (!json.has("skierId") || !json.has("resortId") || !json.has("liftId") ||
              !json.has("seasonId") || !json.has("dayId") || !json.has("time")) {
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        resp.getWriter().write("{\"error\": \"Missing required fields\"}");
        return;
      }

      // Validate numeric fields
      int skierId = json.getInt("skierId");
      int resortId = json.getInt("resortId");
      int liftId = json.getInt("liftId");
      int seasonId = json.getInt("seasonId");
      int dayId = json.getInt("dayId");
      int time = json.getInt("time");

      if (skierId <= 0 || resortId <= 0 || liftId <= 0 || seasonId <= 0 || dayId <= 0 || time < 0 || time > 360) {
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        resp.getWriter().write("{\"error\": \"Invalid values for fields\"}");
        return;
      }

      // Success response
      resp.setStatus(HttpServletResponse.SC_CREATED);
      JSONObject responseJson = new JSONObject();
      responseJson.put("message", "Lift ride recorded successfully");
      responseJson.put("data", json);

      PrintWriter out = resp.getWriter();
      out.write(responseJson.toString());
      out.flush();

    } catch (JSONException e) {
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      resp.getWriter().write("{\"error\": \"Invalid JSON format\"}");
    }
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
          throws ServletException, IOException {

    response.setContentType("text/html");
    PrintWriter out = response.getWriter();
    out.println("<h1>It works!!!!</h1>");
  }
}
