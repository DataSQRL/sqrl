package com.datasqrl.graphql.server;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.datasqrl.graphql.jdbc.GenericJdbcClient;
import com.datasqrl.graphql.jdbc.JdbcContext;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import lombok.SneakyThrows;
import org.jooq.tools.json.JSONObject;

public class ServiceHandler implements
    RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {

  public static final ObjectMapper mapper = new ObjectMapper();

  //Hold some classes in memory for AWS SnapStart
  // TODO: Check to see what is really needed
  //       Check to see impact on postgres, etc
  public static final GenericJdbcClient client = createClient();
  public static Class clazz = createClass();
  //used for native compilation
  public static Class h2Driver = org.h2.Driver.class;
  public static Class postgres = org.postgresql.Driver.class;
  public static final GraphQL graphQL = create(client);

  public static final String CONFIG_JSON = "config.json";
  public static final String MODEL_JSON = "model.json";

  @SneakyThrows
  private static Class createClass() {
    Map jdbcConfig = mapper.readValue(new File(CONFIG_JSON), Map.class);

    clazz = Class.forName((String) jdbcConfig.get("driver"));

    return clazz;
  }

  @SneakyThrows
  private static GenericJdbcClient createClient() {
    Map jdbcConfig = mapper.readValue(new File(CONFIG_JSON), Map.class);

    clazz = Class.forName((String)jdbcConfig.get("driver"));

    Connection connection = DriverManager.getConnection(
        (String)jdbcConfig.get("url"), (String)jdbcConfig.get("user"), (String) jdbcConfig.get("password"));

    return new GenericJdbcClient(connection);
  }

  @SneakyThrows
  private static GraphQL create(GenericJdbcClient client) {
    RootGraphqlModel model = mapper.readValue(new File(MODEL_JSON), RootGraphqlModel.class);

    return model.accept(
        new BuildGraphQLEngine(),
        new JdbcContext(client));
  }

  /**
   * Native exec
   */
  public static void main(String[] args) {
    var http = HttpClient.newBuilder().build();
    var context = System.getenv();
    var runtime = context.get("AWS_LAMBDA_RUNTIME_API");
    var mapType = new TypeReference<Map<String, Object>>() {
    };

    while (true) {
      try {
        HttpResponse<String> invocation = getNextInvocation(http,
            String.format("http://%s/2018-06-01/runtime/invocation/next", runtime));
        String requestId = invocation.headers().map().get("lambda-runtime-aws-request-id").get(0);
        String invocationUrl = String.format("http://%s/2018-06-01/runtime/invocation/%s", runtime, requestId);
        try {
          Map<String, Object> event = mapper.readValue(invocation.body(), mapType);
          String rawPath = (String) event.get("rawPath");
          if (rawPath == null) {
            throw new Exception("missing \"rawPath\" in event");
          }

          switch (rawPath) {
            case "/noop":
              postLambdaApi(http, invocationUrl + "/response", "{ \"message\": \"noop\"}");
              break;
            default:
              String body = (String)event.get("body");
              Map<String, Object> query =
                  mapper.readValue(body, Map.class);
              ExecutionInput.Builder input = ExecutionInput.newExecutionInput()
                  .query((String) query.get("query"));
              if (query.get("variables") != null) {
                input.variables((Map<String, Object>) query.get("variables"));
              }

              ExecutionResult result = graphQL.execute(input.build());

              postLambdaApi(http, invocationUrl + "/response",
                  new JSONObject(result.toSpecification()).toString());
          }
        } catch (Exception e) {
          postLambdaApi(http, invocationUrl + "/error", e.getMessage());
        }
      } catch (Exception e) {
//        System.err.println(e.getMessage());
      }
    }
  }
  @SneakyThrows
  private static HttpResponse<String> getNextInvocation(HttpClient http, String uri) throws IOException, InterruptedException {
    return http.send(
        HttpRequest.newBuilder().uri(URI.create(uri)).GET().build(),
        HttpResponse.BodyHandlers.ofString(UTF_8)
    );
  }
  private static HttpResponse<String> postLambdaApi(HttpClient http, String uri, String body) throws Exception {
    return http.send(
        HttpRequest.newBuilder()
            .uri(URI.create(uri))
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build(),
        HttpResponse.BodyHandlers.ofString(UTF_8)
    );
  }

  @SneakyThrows
  @Override
  public APIGatewayV2HTTPResponse handleRequest(APIGatewayV2HTTPEvent apiGatewayV2HTTPEvent,
      Context context) {
    Map<String, Object> query =
        mapper.readValue(apiGatewayV2HTTPEvent.getBody(), Map.class);
    ExecutionInput.Builder input = ExecutionInput.newExecutionInput()
        .query((String) query.get("query"));
    if (query.get("variables") != null) {
      input.variables((Map<String, Object>) query.get("variables"));
    }

    ExecutionResult result = graphQL.execute(input.build());

    APIGatewayV2HTTPResponse resp = APIGatewayV2HTTPResponse.builder()
        .withHeaders(Map.of("Content-Type", "application/json"))
        .withBody(new JSONObject(result.toSpecification()).toString())
        .withStatusCode(200)
        .build();
    return resp;
  }
}