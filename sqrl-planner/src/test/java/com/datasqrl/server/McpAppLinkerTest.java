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
import com.datasqrl.server.operation.McpMethodType;
import graphql.language.OperationDefinition.Operation;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;

class McpAppLinkerTest {

  private static final String API_VERSION = "v1";

  private final ErrorCollector errors = ErrorCollector.root();

  @Test
  void givenToolNamedLikeApp_whenLinking_thenLinksAppIgnoringCase() {
    var operations = List.of(tool("getSales", McpMethodType.TOOL, null));

    var linked = link(operations, "GetSales");

    assertThat(linked).singleElement().extracting(ApiOperation::getAppName).isEqualTo("GetSales");
    assertThat(warnings()).isEmpty();
  }

  @Test
  void givenAppName_whenLinking_thenLinksNamedAppIgnoringCase() {
    var operations =
        List.of(
            tool("GetSales", McpMethodType.TOOL, "saleschart"),
            tool("GetDailySales", McpMethodType.APP, "SalesChart"));

    var linked = link(operations, "SalesChart", "GetSales");

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

    var linked = link(operations, "GetSales");

    assertThat(linked).singleElement().extracting(ApiOperation::getAppName).isNull();
  }

  @Test
  void givenAppToolWithoutApp_whenLinking_thenLinksNoApp() {
    var operations = List.of(tool("GetDailySales", McpMethodType.APP, null));

    var linked = link(operations, "SalesChart");

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

    var linked = link(operations, "GetSales");

    assertThat(linked).singleElement().extracting(ApiOperation::getAppName).isNull();
    assertThat(warnings()).singleElement().asString().contains("'GetSales' is not linked");
  }

  @Test
  void givenAppNameOnOperationNotExposedOverMcp_whenLinking_thenFails() {
    var operations = List.of(tool("GetSales", McpMethodType.NONE, "SalesChart"));

    assertThatThrownBy(() -> link(operations, "SalesChart"))
        .hasMessageContaining(
            "The operation 'GetSales' has appName 'SalesChart' but is not exposed over MCP");
  }

  @Test
  void givenUnknownAppName_whenLinking_thenFails() {
    var operations = List.of(tool("GetSales", McpMethodType.TOOL, "Missing"));

    assertThatThrownBy(() -> link(operations, "SalesChart"))
        .hasMessageContaining(
            "The operation 'GetSales' has appName 'Missing', but there is no MCP app with that"
                + " name");
  }

  @Test
  void givenAppNameWithoutAppsSupport_whenLinking_thenFails() {
    var operations = List.of(tool("GetSales", McpMethodType.TOOL, "SalesChart"));

    assertThatThrownBy(() -> linkWithoutApps(operations, true))
        .hasMessageContaining(
            "The operation 'GetSales' has appName 'SalesChart', but API version 'v1' has no MCP"
                + " apps: configure their directory in 'script.api.v1.mcp-apps'");
  }

  @Test
  void givenEmptyAppNameWithoutAppsSupport_whenLinking_thenLinksNoApp() {
    var operations = List.of(tool("GetSales", McpMethodType.TOOL, ""));

    var linked = linkWithoutApps(operations, true);

    assertThat(linked).singleElement().extracting(ApiOperation::getAppName).isNull();
    assertThat(warnings()).isEmpty();
  }

  @Test
  void givenAppToolWithoutAppsSupport_whenLinking_thenWarnsThatNoAppCanCallIt() {
    var operations = List.of(tool("GetDailySales", McpMethodType.APP, null));

    var linked = linkWithoutApps(operations, true);

    assertThat(linked).singleElement().extracting(ApiOperation::getAppName).isNull();
    assertThat(warnings())
        .singleElement()
        .asString()
        .contains(
            "The operation 'GetDailySales' is a tool for MCP apps only (mcp: APP), but API version"
                + " 'v1' has no MCP apps that could call it");
  }

  @Test
  void givenAppNameWithoutMcp_whenLinking_thenLinksNoAppWithoutError() {
    var operations =
        List.of(
            tool("GetSales", McpMethodType.NONE, "SalesChart"),
            tool("GetDailySales", McpMethodType.NONE, ""));

    var linked = linkWithoutApps(operations, false);

    assertThat(linked).extracting(ApiOperation::getAppName).containsOnlyNulls();
    assertThat(warnings()).isEmpty();
  }

  private List<ApiOperation> link(List<ApiOperation> operations, String... appNames) {
    var apps =
        Arrays.stream(appNames).map(name -> new McpAppSource(name, "<html></html>")).toList();
    return McpAppLinker.link(operations, Optional.of(apps), true, API_VERSION, errors);
  }

  private List<ApiOperation> linkWithoutApps(List<ApiOperation> operations, boolean mcpEnabled) {
    return McpAppLinker.link(operations, Optional.empty(), mcpEnabled, API_VERSION, errors);
  }

  private List<String> warnings() {
    return StreamSupport.stream(errors.spliterator(), false)
        .filter(ErrorMessage::isWarning)
        .map(ErrorMessage::getMessage)
        .toList();
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
