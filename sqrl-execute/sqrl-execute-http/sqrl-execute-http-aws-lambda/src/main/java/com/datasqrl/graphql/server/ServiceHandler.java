package com.datasqrl.graphql.server;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.datasqrl.graphql.jdbc.GenericJdbcClient;
import com.datasqrl.graphql.jdbc.JdbcContext;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import java.io.File;
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
  public static final GraphQL graphQL = create(client);

  public static final String CONFIG_JSON = "config.json";
  public static final String MODEL_JSON = "model.json";

  @SneakyThrows
  private static Class createClass() {
    Map jdbcConfig = mapper.readValue(new File(CONFIG_JSON), Map.class);

    clazz = Class.forName((String) jdbcConfig.get("driverName"));

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