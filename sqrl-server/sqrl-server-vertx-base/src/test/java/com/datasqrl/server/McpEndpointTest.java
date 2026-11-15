/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.server.config.ServerConfig;
import com.datasqrl.server.config.ServletConfig;
import com.datasqrl.server.graphql.RootGraphQLModel;
import com.datasqrl.server.operation.ApiOperation;
import com.datasqrl.server.operation.FunctionDefinition;
import com.datasqrl.server.operation.FunctionDefinition.Argument;
import com.datasqrl.server.operation.GraphQLQuery;
import com.datasqrl.server.operation.McpApp;
import com.datasqrl.server.operation.McpMethodType;
import com.datasqrl.util.JsonUtils;
import com.fasterxml.jackson.databind.JsonNode;
import graphql.GraphQL;
import graphql.language.OperationDefinition.Operation;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.spec.McpError;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Drives the MCP endpoint over HTTP, with the MCP Java SDK client and with raw JSON-RPC requests,
 * against a {@link McpBridgeVerticle} backed by an in-memory GraphQL engine.
 */
class McpEndpointTest {

  private static final String MCP_ENDPOINT = "/v1/mcp";
  private static final String JWT_SECRET = "dGVzdA==";

  private static final String SDL =
      """
      type Query {
        sensors(sensorId: Int!, limit: Int): [Sensor]
        failing: String
        forbidden: String
      }
      type Mutation {
        addReading(sensorId: Int!, temperature: Float!): Sensor
      }
      type Sensor {
        sensorId: Int
        temperature: Float
      }
      """;

  private final HttpClient httpClient =
      HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
  private Vertx vertx;
  private String baseUrl;

  @BeforeEach
  void setUp() {
    vertx = Vertx.vertx();
  }

  @AfterEach
  @SneakyThrows
  void tearDown() {
    await(vertx.close());
  }

  @Nested
  class WithoutAuth {

    @BeforeEach
    void startServer() {
      McpEndpointTest.this.startServer(new ServerConfig(), List.of());
    }

    @Test
    void givenClient_whenInitializing_thenReportsServerInfoAndOnlyToolsCapability() {
      try (var client = createClient(null)) {
        var result = client.initialize();

        assertThat(result.serverInfo().name()).isEqualTo("datasqrl-mcp-server");
        assertThat(result.protocolVersion()).isEqualTo("2025-11-25");
        assertThat(result.capabilities().tools()).isNotNull();
        assertThat(result.capabilities().resources()).isNull();
        assertThat(result.capabilities().extensions()).isNullOrEmpty();
      }
    }

    @Test
    void givenToolOperations_whenListingTools_thenReturnsOnlyMcpToolsWithSchemasAndHints() {
      try (var client = createClient(null)) {
        client.initialize();

        var tools = client.listTools().tools();

        assertThat(tools)
            .extracting(McpSchema.Tool::name)
            .containsExactlyInAnyOrder("GetSensors", "AddReading", "GetFailing", "GetForbidden");

        var getSensors = tool(tools, "GetSensors");
        assertThat(getSensors.description()).isEqualTo("Returns the readings of a sensor.");
        assertThat(getSensors.annotations().readOnlyHint()).isTrue();
        assertThat(getSensors.inputSchema())
            .containsEntry("type", "object")
            .containsEntry("required", List.of("sensorId"))
            .containsEntry(
                "properties",
                Map.of("sensorId", Map.of("type", "integer"), "limit", Map.of("type", "integer")));

        var addReading = tool(tools, "AddReading");
        assertThat(addReading.description()).isEqualTo("Invokes AddReading mutation");
        assertThat(addReading.annotations().readOnlyHint()).isFalse();
        assertThat(tools).allSatisfy(tool -> assertThat(tool.meta()).isNull());
      }
    }

    @Test
    void givenValidArguments_whenCallingTool_thenReturnsResultWithoutOperationNesting() {
      try (var client = createClient(null)) {
        client.initialize();

        var result =
            client.callTool(new CallToolRequest("GetSensors", Map.of("sensorId", 7, "limit", 2)));

        assertThat(result.isError()).isFalse();
        assertThat(json(result))
            .isEqualTo(
                JsonUtils.MAPPER.valueToTree(
                    List.of(
                        Map.of("sensorId", 7, "temperature", 21.5),
                        Map.of("sensorId", 7, "temperature", 21.5))));
      }
    }

    @Test
    void givenMutationTool_whenCallingTool_thenExecutesMutation() {
      try (var client = createClient(null)) {
        client.initialize();

        var result =
            client.callTool(
                new CallToolRequest("AddReading", Map.of("sensorId", 3, "temperature", 18.25)));

        assertThat(result.isError()).isFalse();
        assertThat(json(result))
            .isEqualTo(JsonUtils.MAPPER.valueToTree(Map.of("sensorId", 3, "temperature", 18.25)));
      }
    }

    @Test
    void givenExplicitNullForOptionalArgument_whenCallingTool_thenTreatsArgumentAsOmitted() {
      try (var client = createClient(null)) {
        client.initialize();
        var arguments = new HashMap<String, Object>();
        arguments.put("sensorId", 7);
        arguments.put("limit", null);

        var result = client.callTool(new CallToolRequest("GetSensors", arguments));

        assertThat(result.isError()).isFalse();
        assertThat(json(result)).hasSize(1);
      }
    }

    @Test
    void givenArgumentsViolatingSchema_whenCallingTool_thenReturnsValidationToolError() {
      try (var client = createClient(null)) {
        client.initialize();

        var result = client.callTool(new CallToolRequest("GetSensors", Map.of("limit", "many")));

        assertThat(result.isError()).isTrue();
        assertThat(text(result)).startsWith("Validation error: ").contains("sensorId");
      }
    }

    @Test
    void givenGraphQLError_whenCallingTool_thenReturnsToolError() {
      try (var client = createClient(null)) {
        client.initialize();

        var result = client.callTool(new CallToolRequest("GetFailing", Map.of()));

        assertThat(result.isError()).isTrue();
        assertThat(text(result)).startsWith("Tool Error[[failing]]: ").contains("sensor offline");
      }
    }

    @Test
    void givenOperationThatSetsHttpStatus_whenCallingTool_thenRespondsWithThatStatus() {
      var response =
          post(
              "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\","
                  + "\"params\":{\"name\":\"GetForbidden\",\"arguments\":{}}}");

      assertThat(response.statusCode()).isEqualTo(403);
      assertThat(body(response).at("/result/isError").asBoolean()).isTrue();
    }

    @Test
    void givenUnknownTool_whenCallingTool_thenFailsWithInvalidParams() {
      try (var client = createClient(null)) {
        client.initialize();

        assertThatThrownBy(() -> client.callTool(new CallToolRequest("GetHidden", Map.of())))
            .isInstanceOfSatisfying(
                McpError.class,
                error -> assertThat(error.getJsonRpcError().code()).isEqualTo(-32602));
      }
    }

    @Test
    void givenClient_whenPinging_thenSucceeds() {
      try (var client = createClient(null)) {
        client.initialize();

        assertThat(client.ping()).isNotNull();
      }
    }

    @ParameterizedTest
    @ValueSource(strings = {"2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25"})
    void givenSupportedProtocolVersion_whenInitializing_thenEchoesRequestedVersion(String version) {
      var response = post(initializeRequest(version));

      assertThat(response.statusCode()).isEqualTo(200);
      assertThat(body(response).at("/result/protocolVersion").asText()).isEqualTo(version);
    }

    @Test
    void givenUnsupportedProtocolVersion_whenInitializing_thenReturnsLatestVersion() {
      var response = post(initializeRequest("2099-01-01"));

      assertThat(response.statusCode()).isEqualTo(200);
      assertThat(body(response).at("/result/protocolVersion").asText()).isEqualTo("2025-11-25");
    }

    @ParameterizedTest
    @ValueSource(
        strings = {"prompts/list", "resources/list", "resources/templates/list", "resources/read"})
    void givenUnsupportedMethod_whenRequesting_thenFailsWithMethodNotFound(String method) {
      var response = post("{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"%s\"}".formatted(method));

      assertThat(response.statusCode()).isEqualTo(200);
      assertThat(body(response).at("/id").asInt()).isEqualTo(5);
      assertThat(body(response).at("/error/code").asInt()).isEqualTo(-32601);
    }

    @Test
    void givenToolCallWithoutParams_whenRequesting_thenFailsWithInternalError() {
      var response = post("{\"jsonrpc\":\"2.0\",\"id\":6,\"method\":\"tools/call\"}");

      assertThat(response.statusCode()).isEqualTo(200);
      assertThat(body(response).at("/id").asInt()).isEqualTo(6);
      assertThat(body(response).at("/error/code").asInt()).isEqualTo(-32603);
    }

    @Test
    void givenNotification_whenPosting_thenAcceptsWithoutBody() {
      var response = post("{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}");

      assertThat(response.statusCode()).isEqualTo(202);
      assertThat(response.body()).isEmpty();
    }

    @Test
    void givenClientResponse_whenPosting_thenAcceptsWithoutBody() {
      var response = post("{\"jsonrpc\":\"2.0\",\"id\":9,\"result\":{}}");

      assertThat(response.statusCode()).isEqualTo(202);
      assertThat(response.body()).isEmpty();
    }

    @Test
    void givenObjectThatIsNoJsonRpcMessage_whenPosting_thenFailsWithInvalidRequest() {
      var response = post("{\"jsonrpc\":\"2.0\"}");

      assertThat(response.statusCode()).isEqualTo(400);
      assertThat(body(response).at("/error/code").asInt()).isEqualTo(-32600);
    }

    @Test
    void givenMalformedJson_whenPosting_thenFailsWithParseError() {
      var response = post("{\"jsonrpc\":");

      assertThat(response.statusCode()).isEqualTo(400);
      assertThat(body(response).at("/error/code").asInt()).isEqualTo(-32700);
      assertThat(body(response).get("id").isNull()).isTrue();
    }

    @Test
    void givenBatchRequest_whenPosting_thenRejectsWithBadRequest() {
      var response = post("[" + initializeRequest("2025-11-25") + "]");

      assertThat(response.statusCode()).isEqualTo(400);
      assertThat(body(response).at("/error/code").asInt()).isEqualTo(-32600);
    }

    @Test
    @SneakyThrows
    void givenGetRequest_whenOpeningStream_thenRespondsMethodNotAllowed() {
      var response =
          httpClient.send(
              HttpRequest.newBuilder(URI.create(baseUrl + MCP_ENDPOINT))
                  .header("Accept", "text/event-stream")
                  .GET()
                  .build(),
              HttpResponse.BodyHandlers.ofString());

      assertThat(response.statusCode()).isEqualTo(405);
    }
  }

  @Nested
  class WithApps {

    private static final String SALES_CHART =
        "<!doctype html><html><body><script>render()</script></body></html>";

    @BeforeEach
    void startServer() {
      var model = createModel();
      var operations =
          model.getOperations().stream()
              .map(
                  operation ->
                      switch (operation.getName()) {
                        case "GetSensors" -> operation.toBuilder().appName("SalesChart").build();
                        case "AddReading" ->
                            operation.toBuilder()
                                .mcpMethod(McpMethodType.APP)
                                .appName("SalesChart")
                                .build();
                        case "GetFailing" ->
                            operation.toBuilder().mcpMethod(McpMethodType.APP).build();
                        default -> operation;
                      })
              .toList();
      McpEndpointTest.this.startServer(
          new ServerConfig(),
          List.of(),
          RootGraphQLModel.builder()
              .operations(operations)
              .mcpApps(List.of(McpApp.of("SalesChart", SALES_CHART)))
              .build());
    }

    @Test
    void givenApps_whenInitializing_thenAdvertisesResourcesAndAppsExtension() {
      try (var client = createClient(null)) {
        var result = client.initialize();

        assertThat(result.capabilities().resources()).isNotNull();
        assertThat(result.capabilities().extensions())
            .containsEntry(
                "io.modelcontextprotocol/ui",
                Map.of("mimeTypes", List.of("text/html;profile=mcp-app")));
      }
    }

    @Test
    void givenApps_whenListingTools_thenEmitsVisibilityAndResourceUriOfLinkedApp() {
      try (var client = createClient(null)) {
        client.initialize();

        var tools = client.listTools().tools();

        assertThat(tools)
            .extracting(McpSchema.Tool::name)
            .containsExactlyInAnyOrder("GetSensors", "AddReading", "GetFailing", "GetForbidden");
        assertThat(tool(tools, "GetSensors").meta())
            .isEqualTo(
                Map.of(
                    "ui",
                    Map.of(
                        "resourceUri",
                        "ui://apps/SalesChart",
                        "visibility",
                        List.of("model", "app"))));
        assertThat(tool(tools, "AddReading").meta())
            .isEqualTo(
                Map.of(
                    "ui",
                    Map.of("resourceUri", "ui://apps/SalesChart", "visibility", List.of("app"))));
        assertThat(tool(tools, "GetFailing").meta())
            .isEqualTo(Map.of("ui", Map.of("visibility", List.of("app"))));
        assertThat(tool(tools, "GetForbidden").meta())
            .isEqualTo(Map.of("ui", Map.of("visibility", List.of("model", "app"))));
      }
    }

    @Test
    void givenApps_whenListingResources_thenListsEachApp() {
      try (var client = createClient(null)) {
        client.initialize();

        var resources = client.listResources().resources();

        assertThat(resources)
            .singleElement()
            .satisfies(
                resource -> {
                  assertThat(resource.uri()).isEqualTo("ui://apps/SalesChart");
                  assertThat(resource.name()).isEqualTo("SalesChart");
                  assertThat(resource.mimeType()).isEqualTo("text/html;profile=mcp-app");
                });
      }
    }

    @Test
    void givenAppUri_whenReadingResource_thenReturnsAppHtmlAsText() {
      try (var client = createClient(null)) {
        client.initialize();

        var result = client.readResource(new McpSchema.ReadResourceRequest("ui://apps/SalesChart"));

        assertThat(result.contents())
            .singleElement()
            .isInstanceOfSatisfying(
                McpSchema.TextResourceContents.class,
                contents -> {
                  assertThat(contents.uri()).isEqualTo("ui://apps/SalesChart");
                  assertThat(contents.mimeType()).isEqualTo("text/html;profile=mcp-app");
                  assertThat(contents.text()).isEqualTo(SALES_CHART);
                });
      }
    }

    @Test
    void givenUnknownUri_whenReadingResource_thenFails() {
      try (var client = createClient(null)) {
        client.initialize();

        assertThatThrownBy(
                () -> client.readResource(new McpSchema.ReadResourceRequest("ui://apps/Missing")))
            .isInstanceOf(McpError.class);
      }
    }

    @Test
    void givenAppOnlyTool_whenCallingTool_thenExecutesOperation() {
      try (var client = createClient(null)) {
        client.initialize();

        var result =
            client.callTool(
                new CallToolRequest("AddReading", Map.of("sensorId", 3, "temperature", 18.25)));

        assertThat(result.isError()).isFalse();
        assertThat(json(result))
            .isEqualTo(JsonUtils.MAPPER.valueToTree(Map.of("sensorId", 3, "temperature", 18.25)));
      }
    }
  }

  @Nested
  class WithAppToolWithoutAppsSupport {

    @BeforeEach
    void startServer() {
      var operations =
          createModel().getOperations().stream()
              .map(
                  operation ->
                      operation.getName().equals("GetFailing")
                          ? operation.toBuilder().mcpMethod(McpMethodType.APP).build()
                          : operation)
              .toList();
      McpEndpointTest.this.startServer(
          new ServerConfig(), List.of(), RootGraphQLModel.builder().operations(operations).build());
    }

    @Test
    void givenAppOnlyTool_whenListingTools_thenOnlyItHasAppVisibility() {
      try (var client = createClient(null)) {
        var capabilities = client.initialize().capabilities();
        var tools = client.listTools().tools();

        assertThat(capabilities.resources()).isNull();
        assertThat(tool(tools, "GetFailing").meta())
            .isEqualTo(Map.of("ui", Map.of("visibility", List.of("app"))));
        assertThat(tools)
            .filteredOn(tool -> !tool.name().equals("GetFailing"))
            .allSatisfy(tool -> assertThat(tool.meta()).isNull());
      }
    }
  }

  @Nested
  class WithAuth {

    private JWTAuth jwtAuth;

    @BeforeEach
    void startServer() {
      jwtAuth =
          JWTAuth.create(
              vertx,
              new JWTAuthOptions()
                  .addPubSecKey(
                      new PubSecKeyOptions().setAlgorithm("HS256").setBuffer(JWT_SECRET)));
      var config = new ServerConfig();
      config.setAuth(
          Map.of(
              "issuers",
              List.of(
                  Map.of(
                      "issuer", "https://issuer.example.com",
                      "audiences", List.of("datasqrl"),
                      "jwksUri", "https://issuer.example.com/jwks"))));
      McpEndpointTest.this.startServer(config, List.of(jwtAuth));
    }

    @Test
    void givenNoToken_whenPosting_thenRejectsWithResourceMetadataChallenge() {
      var response = post(initializeRequest("2025-11-25"));

      assertThat(response.statusCode()).isEqualTo(401);
      assertThat(response.headers().firstValue("WWW-Authenticate"))
          .hasValueSatisfying(value -> assertThat(value).startsWith("Bearer resource_metadata="));
    }

    @Test
    void givenValidToken_whenListingTools_thenSucceeds() {
      var token = jwtAuth.generateToken(new JsonObject().put("sub", "agent"));

      try (var client = createClient(token)) {
        client.initialize();

        assertThat(client.listTools().tools()).hasSize(4);
      }
    }
  }

  @SneakyThrows
  private void startServer(ServerConfig config, List<AuthenticationProvider> authProviders) {
    startServer(config, authProviders, createModel());
  }

  @SneakyThrows
  private void startServer(
      ServerConfig config, List<AuthenticationProvider> authProviders, RootGraphQLModel model) {
    config.setServletConfig(new ServletConfig());
    var graphQLServerVerticle = mock(GraphQLServerVerticle.class);
    when(graphQLServerVerticle.getGraphQLEngine()).thenReturn(createGraphQL());

    var router = Router.router(vertx);
    router.route().handler(BodyHandler.create());
    var verticle =
        new McpBridgeVerticle(router, config, "v1", model, authProviders, graphQLServerVerticle);
    await(vertx.deployVerticle(verticle));
    HttpServer server = await(vertx.createHttpServer().requestHandler(router).listen(0));
    baseUrl = "http://localhost:" + server.actualPort();
  }

  private McpSyncClient createClient(String token) {
    var transport = HttpClientStreamableHttpTransport.builder(baseUrl).endpoint(MCP_ENDPOINT);
    if (token != null) {
      transport.addHttpRequestCustomizer(
          (request, method, uri, body, context) ->
              request.header("Authorization", "Bearer " + token));
    }
    return McpClient.sync(transport.build()).requestTimeout(Duration.ofSeconds(10)).build();
  }

  @SneakyThrows
  private HttpResponse<String> post(String body) {
    return httpClient.send(
        HttpRequest.newBuilder(URI.create(baseUrl + MCP_ENDPOINT))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json, text/event-stream")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private static String initializeRequest(String protocolVersion) {
    return """
        {"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"%s",\
        "capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}"""
        .formatted(protocolVersion);
  }

  @SneakyThrows
  private static JsonNode body(HttpResponse<String> response) {
    return JsonUtils.MAPPER.readTree(response.body());
  }

  private static McpSchema.Tool tool(List<McpSchema.Tool> tools, String name) {
    return tools.stream().filter(tool -> tool.name().equals(name)).findFirst().orElseThrow();
  }

  private static String text(CallToolResult result) {
    assertThat(result.content()).hasSize(1).first().isInstanceOf(TextContent.class);
    return ((TextContent) result.content().get(0)).text();
  }

  @SneakyThrows
  private static JsonNode json(CallToolResult result) {
    return JsonUtils.MAPPER.readTree(text(result));
  }

  @SneakyThrows
  private static <T> T await(Future<T> future) {
    return future.toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
  }

  private static GraphQL createGraphQL() {
    var wiring =
        RuntimeWiring.newRuntimeWiring()
            .type(
                "Query",
                type ->
                    type.dataFetcher(
                            "sensors",
                            env -> {
                              int sensorId = env.getArgument("sensorId");
                              int limit =
                                  env.containsArgument("limit") ? env.getArgument("limit") : 1;
                              return IntStream.range(0, limit)
                                  .mapToObj(i -> Map.of("sensorId", sensorId, "temperature", 21.5))
                                  .toList();
                            })
                        .dataFetcher(
                            "failing",
                            env -> {
                              throw new IllegalStateException("sensor offline");
                            })
                        .dataFetcher(
                            "forbidden",
                            env -> {
                              // Like a required auth claim that the token does not have
                              RoutingContext ctx =
                                  env.getGraphQlContext().get(RoutingContext.class);
                              ctx.response().setStatusCode(403);
                              throw new IllegalStateException("Forbidden");
                            }))
            .type(
                "Mutation",
                type ->
                    type.dataFetcher(
                        "addReading",
                        env ->
                            Map.of(
                                "sensorId",
                                env.getArgument("sensorId"),
                                "temperature",
                                env.getArgument("temperature"))))
            .build();
    var schema = new SchemaGenerator().makeExecutableSchema(new SchemaParser().parse(SDL), wiring);
    return GraphQL.newGraphQL(schema).build();
  }

  private static RootGraphQLModel createModel() {
    var getSensors =
        operation(
                "GetSensors",
                "Returns the readings of a sensor.",
                Map.of("sensorId", argument("integer"), "limit", argument("integer")),
                List.of("sensorId"),
                "query GetSensors($sensorId: Int!, $limit: Int) {"
                    + " sensors(sensorId: $sensorId, limit: $limit) { sensorId temperature } }",
                Operation.QUERY)
            .build();
    var addReading =
        operation(
                "AddReading",
                null,
                Map.of("sensorId", argument("integer"), "temperature", argument("number")),
                List.of("sensorId", "temperature"),
                "mutation AddReading($sensorId: Int!, $temperature: Float!) {"
                    + " addReading(sensorId: $sensorId, temperature: $temperature)"
                    + " { sensorId temperature } }",
                Operation.MUTATION)
            .build();
    var getFailing =
        operation(
                "GetFailing",
                "Always fails.",
                Map.of(),
                List.of(),
                "query GetFailing { failing }",
                Operation.QUERY)
            .build();
    var getForbidden =
        operation(
                "GetForbidden",
                "Requires a claim.",
                Map.of(),
                List.of(),
                "query GetForbidden { forbidden }",
                Operation.QUERY)
            .build();
    var getHidden =
        operation(
                "GetHidden",
                "Not exposed over MCP.",
                Map.of(),
                List.of(),
                "query GetHidden { failing }",
                Operation.QUERY)
            .mcpMethod(McpMethodType.NONE)
            .build();
    return RootGraphQLModel.builder()
        .operations(
            new ArrayList<>(List.of(getSensors, addReading, getFailing, getForbidden, getHidden)))
        .build();
  }

  private static ApiOperation.ApiOperationBuilder operation(
      String name,
      String description,
      Map<String, Argument> properties,
      List<String> required,
      String query,
      Operation operationType) {
    var function =
        FunctionDefinition.builder()
            .name(name)
            .description(description)
            .parameters(
                FunctionDefinition.Parameters.builder()
                    .type("object")
                    .properties(properties)
                    .required(required)
                    .build())
            .build();
    return ApiOperation.getBuilder(function, new GraphQLQuery(query, name, operationType));
  }

  private static Argument argument(String type) {
    var argument = new Argument();
    argument.setType(type);
    return argument;
  }
}
