package com.datasqrl.graphql.server;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.datasqrl.graphql.jdbc.GenericJdbcClient;
import com.datasqrl.graphql.jdbc.JdbcContext;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.ExecutionResult;
import graphql.GraphQL;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import lombok.SneakyThrows;

public class ServiceHandler implements
    RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {

  static final ObjectMapper mapper = new ObjectMapper();
  static final GraphQL graphQL = create();

  public static final String CONFIG_JSON = "config.json";
  public static final String MODEL_JSON = "model.json";

  @SneakyThrows
  private static GraphQL create() {
    Map jdbcConfig = mapper.readValue(new File(CONFIG_JSON),
        Map.class);
    RootGraphqlModel model = mapper.readValue(new File(MODEL_JSON), RootGraphqlModel.class);

    Connection connection = DriverManager.getConnection(
        (String)jdbcConfig.get("dbURL"), (String)jdbcConfig.get("user"), (String) jdbcConfig.get("password"));

    GraphQL graphQL = model.accept(
        new SqrlGraphQLServer(),
        new JdbcContext(new GenericJdbcClient(connection)));
    return graphQL;
  }

  @SneakyThrows
  @Override
  public APIGatewayV2HTTPResponse handleRequest(APIGatewayV2HTTPEvent apiGatewayV2HTTPEvent,
      Context context) {

    ExecutionResult result = graphQL.execute(apiGatewayV2HTTPEvent.getBody());

    return APIGatewayV2HTTPResponse.builder()
        .withBody(mapper.writeValueAsString(result.getData()))
        .withStatusCode(200)
        .build();
  }
}