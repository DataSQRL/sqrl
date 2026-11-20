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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class McpAppLoaderTest {

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

    var apps = McpAppLoader.load(tempDir, "apps", errors);

    assertThat(apps)
        .containsExactly(
            new McpAppSource("SalesChart", APP_HTML),
            new McpAppSource("orders", "<html>orders</html>"));
    assertThat(warnings()).isEmpty();
  }

  @Test
  void givenMissingDirectory_whenLoading_thenFails() {
    assertThatThrownBy(() -> McpAppLoader.load(tempDir.resolve("missing"), "api/apps", errors))
        .hasMessageContaining(
            "The MCP apps directory 'api/apps' does not exist or has no MCP apps");
  }

  @Test
  @SneakyThrows
  void givenDirectoryWithoutTopLevelHtmlFiles_whenLoading_thenFails() {
    Files.writeString(tempDir.resolve("README.md"), "# Apps");
    Files.createDirectories(tempDir.resolve("src"));
    Files.writeString(tempDir.resolve("src/Nested.html"), APP_HTML);

    assertThatThrownBy(() -> McpAppLoader.load(tempDir, "api/apps", errors))
        .hasMessageContaining(
            "The MCP apps directory 'api/apps' does not exist or has no MCP apps");
  }

  @Test
  void givenAppNamesThatDifferOnlyInCase_whenLoading_thenFails() {
    var files = new LinkedHashMap<String, String>();
    files.put("SalesChart.html", APP_HTML);
    files.put("saleschart.html", APP_HTML);

    assertThatThrownBy(() -> McpAppLoader.toApps(files, "apps", errors))
        .hasMessageContaining(
            "The MCP apps 'SalesChart' and 'saleschart' in 'apps' have names that differ only in"
                + " case");
  }

  @ParameterizedTest
  @ValueSource(strings = {"Sales Chart.html", ".html", "sales#chart.html", "-sales.html"})
  void givenFileNameThatIsNoValidAppName_whenLoading_thenFails(String fileName) {
    assertThatThrownBy(() -> McpAppLoader.toApps(Map.of(fileName, APP_HTML), "apps", errors))
        .hasMessageContaining(
            "The MCP app file '%s' in 'apps' does not have a valid app name".formatted(fileName));
  }

  @ParameterizedTest
  @ValueSource(strings = {"SalesChart", "_sales", "sales-chart_2", "sales.v2"})
  void givenValidAppName_whenLoading_thenLoadsApp(String name) {
    var apps = McpAppLoader.toApps(Map.of(name + ".html", APP_HTML), "apps", errors);

    assertThat(apps).containsExactly(new McpAppSource(name, APP_HTML));
  }

  @Test
  void givenAppLoadingExternalResource_whenLoading_thenWarns() {
    var html =
        "<html><head><link rel=\"stylesheet\" href=\"styles.css\"></head>"
            + "<body><img src='data:image/png;base64,AAAA'></body></html>";

    var apps = McpAppLoader.toApps(Map.of("Report.html", html), "apps", errors);

    assertThat(apps).extracting(McpAppSource::name).containsExactly("Report");
    assertThat(warnings())
        .singleElement()
        .asString()
        .contains("The MCP app 'Report' loads 'styles.css', so it is not self-contained");
  }

  @Test
  void givenHtml_whenFindingExternalReference_thenIgnoresInlineAndDataResources() {
    assertThat(McpAppLoader.externalReference(APP_HTML)).isEmpty();
    assertThat(McpAppLoader.externalReference("<img alt=\"x\" src=\"DATA:image/gif;base64,R0\">"))
        .isEmpty();
    assertThat(McpAppLoader.externalReference("<SCRIPT type=module SRC=/assets/app.js></SCRIPT>"))
        .contains("/assets/app.js");
    assertThat(McpAppLoader.externalReference("<script src=\"https://cdn.example.com/x.js\">"))
        .contains("https://cdn.example.com/x.js");
  }

  @Test
  void givenMarkupInScriptStyleOrComment_whenFindingExternalReference_thenIgnoresIt() {
    var html =
        """
        <html>
          <head>
            <style>.logo::after { content: '<img src="logo.png">'; }</style>
            <!-- <link rel="stylesheet" href="old.css"> -->
          </head>
          <body>
            <script>
              list.innerHTML = items.map((item) => `<img src="${item.url}">`).join("");
              const loader = '<script src="https://cdn.example.com/x.js">';
            </SCRIPT>
          </body>
        </html>
        """;

    assertThat(McpAppLoader.externalReference(html)).isEmpty();
    assertThat(McpAppLoader.externalReference(html + "<img src=\"chart.png\">"))
        .contains("chart.png");
  }

  private List<String> warnings() {
    return StreamSupport.stream(errors.spliterator(), false)
        .filter(ErrorMessage::isWarning)
        .map(ErrorMessage::getMessage)
        .toList();
  }
}
