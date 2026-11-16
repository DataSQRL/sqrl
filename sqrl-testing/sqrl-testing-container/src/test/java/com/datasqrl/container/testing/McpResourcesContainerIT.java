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
package com.datasqrl.container.testing;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.dialect.Dialects;
import io.jsonwebtoken.Jwts;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.spec.McpError;
import io.modelcontextprotocol.spec.McpSchema;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.crypto.spec.SecretKeySpec;
import lombok.SneakyThrows;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Reads MCP resources of operations marked {@code @api(mcp: RESOURCE)} through a server with JWT
 * authentication, and checks the resource listings against the MCP schema.
 */
class McpResourcesContainerIT {

  @RegisterExtension
  static SqrlContainerExtension sqrl = new SqrlContainerExtension("mcp-resources");

  @RegisterExtension
  static PostgresContainerExtension postgres =
      new PostgresContainerExtension(sqrl, McpResourcesContainerIT::createTables);

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String ISSUER = "my-test-issuer";
  private static final String AUDIENCE = "my-test-audience";
  private static final String SECRET = "testSecretThatIsAtLeast256BitsLong32Chars";
  private static final String MCP_SCHEMA = "mcp/schema-2025-11-25.json";

  private final AtomicInteger requestId = new AtomicInteger();

  private static void createTables(Statement stmt) throws SQLException {
    stmt.execute("CREATE TABLE IF NOT EXISTS \"Person\" (name TEXT, age INTEGER)");
    stmt.execute("INSERT INTO \"Person\" VALUES ('Ada', 30), ('Bob', 30), ('Cy', 40)");
    stmt.execute("CREATE TABLE IF NOT EXISTS \"Notes\" (owner INTEGER, label TEXT)");
    stmt.execute("INSERT INTO \"Notes\" VALUES (1, 'one-a'), (1, 'one-b'), (2, 'two')");
  }

  @Test
  @SneakyThrows
  void givenResourceOperations_whenReadingThroughMcp_thenQueriesRunWithCallerClaims() {
    postgres.compileAndStartServerWithDatabase();

    try (var client = createClient(token(1));
        var otherClient = createClient(token(2))) {
      client.initialize();
      otherClient.initialize();

      assertThat(client.listResources().resources())
          .extracting(McpSchema.Resource::uri)
          .containsExactlyInAnyOrder(
              "sqrl://v1/queries/AllPeople",
              "sqrl://v1/queries/CallerNotes",
              "sqrl://v1/queries/BrokenRows");
      assertThat(client.listResourceTemplates().resourceTemplates())
          .extracting(McpSchema.ResourceTemplate::uriTemplate)
          .containsExactlyInAnyOrder(
              "sqrl://v1/queries/personByAge/{age}", "sqrl://v1/queries/People{?offset,limit}");

      assertThat(read(client, "sqrl://v1/queries/AllPeople").findValuesAsText("name"))
          .containsExactlyInAnyOrder("Ada", "Bob", "Cy");
      assertThat(read(client, "sqrl://v1/queries/personByAge/30").findValuesAsText("name"))
          .containsExactly("Ada", "Bob");
      assertThat(read(client, "sqrl://v1/queries/People?limit=10&offset=1")).hasSize(2);
      assertThat(read(client, "sqrl://v1/queries/People?limit=1")).hasSize(1);

      assertThat(read(client, "sqrl://v1/queries/CallerNotes").findValuesAsText("label"))
          .containsExactly("one-a", "one-b");
      assertThat(read(otherClient, "sqrl://v1/queries/CallerNotes").findValuesAsText("label"))
          .containsExactly("two");

      assertThat(readError(client, "nothing")).isEqualTo(-32002);
      assertThat(readError(client, "sqrl://v1/queries/Unknown")).isEqualTo(-32002);
      assertThat(readError(client, "sqrl://v1/queries/personByAge/abc")).isEqualTo(-32602);
      assertThat(readError(client, "sqrl://v1/queries/BrokenRows")).isEqualTo(-32603);

      var toolResult =
          client.callTool(
              McpSchema.CallToolRequest.builder("GetPerson").arguments(Map.of("limit", 2)).build());
      assertThat(toolResult.isError()).isFalse();
      assertThat(MAPPER.readTree(((McpSchema.TextContent) toolResult.content().get(0)).text()))
          .hasSize(2);
    }

    var token = token(1);
    assertMatchesSchema(rpc(token, "resources/list", null), "ListResourcesResult");
    assertMatchesSchema(
        rpc(token, "resources/templates/list", null), "ListResourceTemplatesResult");
    assertMatchesSchema(
        rpc(token, "resources/read", Map.of("uri", "sqrl://v1/queries/personByAge/30")),
        "ReadResourceResult");
  }

  @Test
  @SneakyThrows
  void givenCustomPrefix_whenListingAndReadingResources_thenUrisUsePrefix() {
    postgres.compileAndStartServerWithDatabase(List.of("package.json", "package-prefix.json"));

    try (var client = createClient(token(1))) {
      client.initialize();

      assertThat(client.listResources().resources())
          .extracting(McpSchema.Resource::uri)
          .contains("acme://data/v1/queries/AllPeople");
      assertThat(client.listResourceTemplates().resourceTemplates())
          .extracting(McpSchema.ResourceTemplate::uriTemplate)
          .contains("acme://data/v1/queries/personByAge/{age}");
      assertThat(read(client, "acme://data/v1/queries/personByAge/40").findValuesAsText("name"))
          .containsExactly("Cy");
      assertThat(readError(client, "sqrl://v1/queries/AllPeople")).isEqualTo(-32002);
    }
    assertMatchesSchema(rpc(token(1), "resources/list", null), "ListResourcesResult");
  }

  @Test
  void givenPrefixWithoutScheme_whenCompiling_thenCompileFailsWithReadableError() {
    assertThatThrownBy(
            () -> sqrl.compileSqrlProject(List.of("package.json", "package-invalid-prefix.json")))
        .isInstanceOf(ContainerError.class)
        .extracting(e -> ((ContainerError) e).getLogs())
        .asString()
        .contains("mcp-resource-uri-prefix")
        .contains("must start with a URI scheme");
  }

  private static McpSyncClient createClient(String token) {
    var transport =
        HttpClientStreamableHttpTransport.builder(sqrl.getBaseUrl())
            .addHttpRequestCustomizer(
                (requestBuilder, method, uri, body, context) ->
                    requestBuilder.header("Authorization", "Bearer " + token))
            .endpoint("/v1/mcp")
            .build();
    return McpClient.sync(transport).requestTimeout(Duration.ofSeconds(30)).build();
  }

  @SneakyThrows
  private static JsonNode read(McpSyncClient client, String uri) {
    var contents = client.readResource(new McpSchema.ReadResourceRequest(uri)).contents();
    assertThat(contents).hasSize(1);
    var content = (McpSchema.TextResourceContents) contents.get(0);
    assertThat(content.uri()).isEqualTo(uri);
    assertThat(content.mimeType()).isEqualTo("application/json");
    return MAPPER.readTree(content.text());
  }

  private static int readError(McpSyncClient client, String uri) {
    try {
      client.readResource(new McpSchema.ReadResourceRequest(uri));
    } catch (McpError e) {
      return e.getJsonRpcError().code();
    }
    throw new AssertionError("Reading " + uri + " should fail");
  }

  /** Sends a JSON-RPC request directly, so the raw result is checked, not the SDK's model. */
  @SneakyThrows
  private JsonNode rpc(String token, String method, Map<String, Object> params) {
    var request =
        MAPPER
            .createObjectNode()
            .put("jsonrpc", "2.0")
            .put("id", requestId.incrementAndGet())
            .put("method", method);
    if (params != null) {
      request.set("params", MAPPER.valueToTree(params));
    }
    var post = new HttpPost(sqrl.getBaseUrl() + "/v1/mcp");
    post.setHeader("Authorization", "Bearer " + token);
    post.setHeader("Accept", "application/json");
    post.setEntity(new StringEntity(request.toString(), ContentType.APPLICATION_JSON));
    try (var response = sqrl.getHttpClient().execute(post)) {
      var body = EntityUtils.toString(response.getEntity());
      assertThat(response.getStatusLine().getStatusCode()).as(body).isEqualTo(200);
      var json = MAPPER.readTree(body);
      assertThat(json.has("result")).as(body).isTrue();
      return json.get("result");
    }
  }

  @SneakyThrows
  private static void assertMatchesSchema(JsonNode result, String definition) {
    var errors =
        mcpSchema(definition)
            .validate(
                result,
                (executionContext, schemaContext) ->
                    executionContext.executionConfig(
                        config -> config.formatAssertionsEnabled(true)));
    assertThat(errors).as("%s: %s", definition, result).isEmpty();
  }

  @SneakyThrows
  private static Schema mcpSchema(String definition) {
    try (var in = McpResourcesContainerIT.class.getClassLoader().getResourceAsStream(MCP_SCHEMA)) {
      var schema = (ObjectNode) MAPPER.readTree(in);
      schema.put("$ref", "#/$defs/" + definition);
      return SchemaRegistry.withDefaultDialect(Dialects.getDraft202012()).getSchema(schema);
    }
  }

  private static String token(long val) {
    var now = Instant.now();
    return Jwts.builder()
        .issuer(ISSUER)
        .audience()
        .add(AUDIENCE)
        .and()
        .issuedAt(Date.from(now))
        .expiration(Date.from(now.plusSeconds(3600)))
        .claim("val", val)
        .signWith(new SecretKeySpec(SECRET.getBytes(UTF_8), "HmacSHA256"))
        .compact();
  }
}
