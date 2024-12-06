package com.datasqrl.tests;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.DatasqrlRun;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
public class LoanTestExtension implements TestExtension {

  @Override
  public void setup() {
  }

  @Override
  public void teardown() {
    test();
  }

  public void test() {

    String graphqlEndpoint = "http://localhost:8888/graphql";
    String mut ="mutation {\n"
        + "  ApplicationUpdates(event: {loan_application_id: 101,\n"
        + "     status: \"underwriting\", message: \"The road goes ever on and on\"}) {\n"
        + "    loan_application_id\n"
        + "    message\n"
        + "    event_time\n"
        + "  }\n"
        + "}";

    String response = executeQuery(graphqlEndpoint, mut);

    assertEquals("{\"data\":{\"MyEvent\":[{\"ID\":1.0}]}}", response);
  }

  //todo move to lib
  private static String executeQuery(String endpoint, String query) {
    HttpClient client = HttpClient.newHttpClient();
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(endpoint))
        .header("Content-Type", "application/graphql")
        .POST(HttpRequest.BodyPublishers.ofString(query))
        .build();

    try {
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      return response.body();
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      return null;
    }
  }
}
