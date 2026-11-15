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

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorMessage;
import com.datasqrl.server.operation.ApiOperation;
import com.datasqrl.server.operation.FunctionDefinition;
import com.datasqrl.server.operation.GraphQLQuery;
import com.datasqrl.server.operation.McpApp;
import com.datasqrl.server.operation.McpMethodType;
import graphql.language.OperationDefinition.Operation;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class McpAppLinkerTest {

  private static final String APP_HTML =
      "<!doctype html><html><body><script>render()</script></body></html>";

  private final ErrorCollector errors = ErrorCollector.root();

  @TempDir Path tempDir;

  @Test
  @SneakyThrows
  void givenAppsDirectory_whenLoading_thenLoadsTopLevelHtmlFilesNamedByFileName() {
    Files.writeString(tempDir.resolve("SalesChart.html"), APP_HTML);
    Files.writeString(tempDir.resolve("orders.HTML"), "<html>orders</html>");
    Files.writeString(tempDir.resolve("README.md"), "# Apps");
    Files.createDirectories(tempDir.resolve("src"));
    Files.writeString(tempDir.resolve("src/Nested.html"), APP_HTML);

    var apps = McpAppLinker.loadApps(tempDir, "apps", errors);

    assertThat(apps)
        .containsExactly(
            new McpApp("SalesChart", "ui://apps/SalesChart", McpApp.MIME_TYPE, APP_HTML),
            new McpApp("orders", "ui://apps/orders", McpApp.MIME_TYPE, "<html>orders</html>"));
    assertThat(McpApp.MIME_TYPE).isEqualTo("text/html;profile=mcp-app");
    assertThat(warnings()).isEmpty();
  }

  @Test
  void givenMissingDirectory_whenLoading_thenFails() {
    assertThatThrownBy(() -> McpAppLinker.loadApps(tempDir.resolve("missing"), "api/apps", errors))
        .hasMessageContaining("The MCP apps directory 'api/apps' does not exist");
  }

  @Test
  void givenAppNamesThatDifferOnlyInCase_whenLoading_thenFails() {
    var files = new LinkedHashMap<String, String>();
    files.put("SalesChart.html", APP_HTML);
    files.put("saleschart.html", APP_HTML);

    assertThatThrownBy(() -> McpAppLinker.toApps(files, "apps", errors))
        .hasMessageContaining(
            "The MCP apps 'SalesChart' and 'saleschart' in 'apps' have names that differ only in"
                + " case");
  }

  @Test
  void givenAppLoadingExternalResource_whenLoading_thenWarns() {
    var html =
        "<html><head><link rel=\"stylesheet\" href=\"styles.css\"></head>"
            + "<body><img src='data:image/png;base64,AAAA'></body></html>";

    var apps = McpAppLinker.toApps(Map.of("Report.html", html), "apps", errors);

    assertThat(apps).extracting(McpApp::name).containsExactly("Report");
    assertThat(warnings())
        .singleElement()
        .asString()
        .contains("The MCP app 'Report' loads 'styles.css', so it is not self-contained");
  }

  @Test
  void givenHtml_whenFindingExternalReference_thenIgnoresInlineAndDataResources() {
    assertThat(McpAppLinker.externalReference(APP_HTML)).isEmpty();
    assertThat(McpAppLinker.externalReference("<img alt=\"x\" src=\"DATA:image/gif;base64,R0\">"))
        .isEmpty();
    assertThat(McpAppLinker.externalReference("<SCRIPT type=module SRC=/assets/app.js></SCRIPT>"))
        .contains("/assets/app.js");
    assertThat(McpAppLinker.externalReference("<script src=\"https://cdn.example.com/x.js\">"))
        .contains("https://cdn.example.com/x.js");
  }

  @Test
  void givenToolNamedLikeApp_whenLinking_thenLinksAppIgnoringCase() {
    var operations = List.of(tool("getSales", McpMethodType.TOOL, null));

    var linked = McpAppLinker.link(operations, List.of(McpApp.of("GetSales", APP_HTML)), errors);

    assertThat(linked).singleElement().extracting(ApiOperation::getAppName).isEqualTo("GetSales");
    assertThat(warnings()).isEmpty();
  }

  @Test
  void givenAppName_whenLinking_thenLinksNamedAppIgnoringCase() {
    var operations =
        List.of(
            tool("GetSales", McpMethodType.TOOL, "saleschart"),
            tool("GetDailySales", McpMethodType.APP, "SalesChart"));

    var linked = McpAppLinker.link(operations, apps("SalesChart", "GetSales"), errors);

    assertThat(linked)
        .extracting(ApiOperation::getAppName)
        .containsExactly("SalesChart", "SalesChart");
    assertThat(warnings())
        .singleElement()
        .asString()
        .contains("The MCP app 'GetSales' is not linked to any tool");
  }

  @Test
  void givenEmptyAppName_whenLinking_thenLinksNoAppEvenIfNameMatches() {
    var operations = List.of(tool("GetSales", McpMethodType.TOOL, ""));

    var linked = McpAppLinker.link(operations, apps("GetSales"), errors);

    assertThat(linked).singleElement().extracting(ApiOperation::getAppName).isNull();
  }

  @Test
  void givenAppToolWithoutApp_whenLinking_thenLinksNoApp() {
    var operations = List.of(tool("GetDailySales", McpMethodType.APP, null));

    var linked = McpAppLinker.link(operations, apps("SalesChart"), errors);

    assertThat(linked)
        .singleElement()
        .satisfies(
            operation -> {
              assertThat(operation.getAppName()).isNull();
              assertThat(operation.getMcpMethod()).isEqualTo(McpMethodType.APP);
            });
  }

  @Test
  void givenOperationNotExposedOverMcp_whenLinking_thenLinksNoAppByName() {
    var operations = List.of(tool("GetSales", McpMethodType.NONE, null));

    var linked = McpAppLinker.link(operations, apps("GetSales"), errors);

    assertThat(linked).singleElement().extracting(ApiOperation::getAppName).isNull();
    assertThat(warnings()).singleElement().asString().contains("'GetSales' is not linked");
  }

  @Test
  void givenAppNameOnOperationNotExposedOverMcp_whenLinking_thenFails() {
    var operations = List.of(tool("GetSales", McpMethodType.NONE, "SalesChart"));

    assertThatThrownBy(() -> McpAppLinker.link(operations, apps("SalesChart"), errors))
        .hasMessageContaining(
            "The operation 'GetSales' has appName 'SalesChart' but is not exposed over MCP");
  }

  @Test
  void givenUnknownAppName_whenLinking_thenFails() {
    var operations = List.of(tool("GetSales", McpMethodType.TOOL, "Missing"));

    assertThatThrownBy(() -> McpAppLinker.link(operations, apps("SalesChart"), errors))
        .hasMessageContaining(
            "The operation 'GetSales' has appName 'Missing', but there is no MCP app with that"
                + " name");
  }

  @Test
  void givenAppNameWithoutAppsSupport_whenLinking_thenFails() {
    var operations = List.of(tool("GetSales", McpMethodType.TOOL, "SalesChart"));

    assertThatThrownBy(() -> McpAppLinker.withoutApps(operations, "v1", errors))
        .hasMessageContaining(
            "The operation 'GetSales' has appName 'SalesChart', but API version 'v1' has no MCP"
                + " apps: configure their directory in 'script.api.v1.mcp-apps'");
  }

  @Test
  void givenEmptyAppNameWithoutAppsSupport_whenLinking_thenLinksNoApp() {
    var operations = List.of(tool("GetSales", McpMethodType.TOOL, ""));

    var linked = McpAppLinker.withoutApps(operations, "v1", errors);

    assertThat(linked).singleElement().extracting(ApiOperation::getAppName).isNull();
  }

  private List<String> warnings() {
    return StreamSupport.stream(errors.spliterator(), false)
        .filter(ErrorMessage::isWarning)
        .map(ErrorMessage::getMessage)
        .toList();
  }

  private static List<McpApp> apps(String... names) {
    return java.util.Arrays.stream(names).map(name -> McpApp.of(name, APP_HTML)).toList();
  }

  private static ApiOperation tool(String name, McpMethodType mcpMethod, String appName) {
    var function =
        FunctionDefinition.builder()
            .name(name)
            .parameters(FunctionDefinition.Parameters.builder().type("object").build())
            .build();
    return ApiOperation.getBuilder(
            function,
            new GraphQLQuery("query " + name + " { sales { id } }", name, Operation.QUERY))
        .mcpMethod(mcpMethod)
        .appName(appName)
        .build();
  }
}
