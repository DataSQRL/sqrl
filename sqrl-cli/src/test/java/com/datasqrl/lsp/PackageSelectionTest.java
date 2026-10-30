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
package com.datasqrl.lsp;

import static com.datasqrl.compile.check.CheckTestProject.MAIN_SCRIPT;
import static com.datasqrl.lsp.TestLanguageServers.TIMEOUT_SECONDS;
import static com.datasqrl.lsp.TestLanguageServers.start;
import static com.datasqrl.lsp.TestLanguageServers.stop;
import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.compile.check.CheckTestProject;
import com.datasqrl.lsp.TestLanguageServers.RecordingClient;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.lsp4j.ClientCapabilities;
import org.eclipse.lsp4j.DidChangeConfigurationParams;
import org.eclipse.lsp4j.DidOpenTextDocumentParams;
import org.eclipse.lsp4j.ExecuteCommandParams;
import org.eclipse.lsp4j.InitializeResult;
import org.eclipse.lsp4j.TextDocumentItem;
import org.eclipse.lsp4j.services.LanguageServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Selects which of several package files that name a script as their main script it uses. */
class PackageSelectionTest {

  @TempDir Path tempDir;

  private CheckTestProject project;
  private final RecordingClient client = new RecordingClient();
  private final AtomicInteger exitCode = new AtomicInteger(-1);
  private LanguageServer server;

  @BeforeEach
  void setUp() throws Exception {
    project = CheckTestProject.create(tempDir);
    var result = new InitializeResult[1];
    server = start(tempDir.resolve("work"), client, new ClientCapabilities(), exitCode, result);
    assertThat(result[0].getCapabilities().getExecuteCommandProvider().getCommands())
        .containsExactly(SqrlWorkspaceService.PACKAGE_FILES_COMMAND);
  }

  @AfterEach
  void tearDown() throws Exception {
    stop(server, exitCode);
  }

  @Test
  void givenSecondPackageFileOfScript_whenSelected_thenChecksTheScriptWithIt() throws Exception {
    // Without the package that the script imports
    var other =
        Files.writeString(
            project.mainScript().resolveSibling("main_package_other.json"),
            "{\"version\": \"1\", \"script\": {\"main\": \"main.sqrl\"}}");
    var packageJson = project.mainScript().resolveSibling("package.json").toUri().toString();
    var otherUri = other.toUri().toString();
    var uri = project.mainScript().toUri().toString();
    server
        .getTextDocumentService()
        .didOpen(new DidOpenTextDocumentParams(new TextDocumentItem(uri, "sqrl", 1, MAIN_SCRIPT)));
    TestLanguageServers.awaitHover(server, uri, 2, 1);

    var packageFiles = packageFiles(uri);
    assertThat(uris(packageFiles, "candidates")).containsExactly(otherUri, packageJson);
    assertThat(uris(packageFiles, "used")).containsExactly(packageJson);

    select(Map.of(uri, otherUri));
    assertThat(client.await(uri, diagnostics -> !diagnostics.isEmpty())).isNotEmpty();
    assertThat(uris(packageFiles(uri), "used")).containsExactly(otherUri);

    // Back to the package file that is found by default, which drops the errors of the other
    select(Map.of());
    client.await(uri, List::isEmpty);
    assertThat(uris(packageFiles(uri), "used")).containsExactly(packageJson);
  }

  private JsonObject packageFiles(String uri) throws Exception {
    var result =
        server
            .getWorkspaceService()
            .executeCommand(
                new ExecuteCommandParams(SqrlWorkspaceService.PACKAGE_FILES_COMMAND, List.of(uri)))
            .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    return JsonParser.parseString(new Gson().toJson(result)).getAsJsonObject();
  }

  private static List<String> uris(JsonObject packageFiles, String name) {
    return packageFiles.getAsJsonArray(name).asList().stream()
        .map(element -> element.getAsString())
        .toList();
  }

  private void select(Map<String, String> selectedPackageFiles) {
    var settings = new JsonObject();
    settings.add("selectedPackageFiles", new Gson().toJsonTree(selectedPackageFiles));
    server.getWorkspaceService().didChangeConfiguration(new DidChangeConfigurationParams(settings));
  }
}
