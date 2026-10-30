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
import static com.datasqrl.compile.check.CheckTestProject.SHARED_SCRIPT;
import static com.datasqrl.lsp.TestLanguageServers.TIMEOUT_SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.tuple;

import com.datasqrl.compile.check.CheckTestProject;
import com.datasqrl.lsp.TestLanguageServers.RecordingClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.lsp4j.ClientCapabilities;
import org.eclipse.lsp4j.CompletionContext;
import org.eclipse.lsp4j.CompletionItem;
import org.eclipse.lsp4j.CompletionParams;
import org.eclipse.lsp4j.CompletionTriggerKind;
import org.eclipse.lsp4j.DidChangeTextDocumentParams;
import org.eclipse.lsp4j.DidOpenTextDocumentParams;
import org.eclipse.lsp4j.InitializeResult;
import org.eclipse.lsp4j.Position;
import org.eclipse.lsp4j.TextDocumentContentChangeEvent;
import org.eclipse.lsp4j.TextDocumentIdentifier;
import org.eclipse.lsp4j.TextDocumentItem;
import org.eclipse.lsp4j.VersionedTextDocumentIdentifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Fills in the variables of templates in scripts from the package files of the project. */
class TemplateVariablesTest {

  @TempDir Path tempDir;

  @Test
  void givenScriptsOfProjectAndPackage_whenOf_thenFindsTheVariablesOfEach() throws Exception {
    var project = CheckTestProject.create(tempDir);
    var packageFile = project.projectRoot().resolve("package.json");
    // The project overrides a variable of the package it includes, and has variables of its own
    Files.writeString(
        packageFile,
        Files.readString(packageFile)
            .replace(
                "\"package\": \"../shared/package.json\"}",
                "\"package\": \"../shared/package.json\", \"config\": {\"interval\": \"1 sec\"}}")
            .replace(
                "\"main\": \"main.sqrl\",",
                "\"main\": \"main.sqrl\", \"config\": {\"kafka\": {\"topic\": \"orders\"}},"));

    assertThat(TemplateVariables.of(project.sharedScript(), List.of(packageFile)))
        .containsExactly(entry("interval", "1 sec"));
    assertThat(TemplateVariables.of(project.mainScript(), List.of(packageFile)))
        .containsExactly(entry("kafka.topic", "orders"));
  }

  @Test
  void givenTemplate_whenNameAt_thenFindsTheVariable() {
    var text = "X := SELECT '{{ kafka.topic }}' AS t;";

    assertThat(TemplateVariables.nameAt(text, text.indexOf("topic"))).contains("kafka.topic");
    assertThat(TemplateVariables.nameAt(text, text.indexOf("SELECT"))).isEmpty();
  }

  @Test
  void givenTemplateInScriptOfPackage_whenCompletionAndHover_thenSuggestsAndDescribesItsVariables()
      throws Exception {
    var project = CheckTestProject.create(tempDir);
    var exitCode = new AtomicInteger(-1);
    var server =
        TestLanguageServers.start(
            tempDir.resolve("work"),
            new RecordingClient(),
            new ClientCapabilities(),
            exitCode,
            new InitializeResult[1]);
    try {
      var documents = server.getTextDocumentService();
      // The script of the package belongs to the project that includes it, once it was checked
      var mainUri = project.mainScript().toUri().toString();
      documents.didOpen(
          new DidOpenTextDocumentParams(new TextDocumentItem(mainUri, "sqrl", 1, MAIN_SCRIPT)));
      TestLanguageServers.awaitHover(server, mainUri, 4, 66);
      var uri = project.sharedScript().toUri().toString();
      var typed = SHARED_SCRIPT.replace("'{{interval}}'", "'{{'");
      documents.didOpen(new DidOpenTextDocumentParams(new TextDocumentItem(uri, "sqrl", 1, typed)));
      var completion =
          new CompletionParams(
              new TextDocumentIdentifier(uri),
              new Position(4, typed.lines().toList().get(4).indexOf("{{") + 2));
      completion.setContext(new CompletionContext(CompletionTriggerKind.TriggerCharacter, "{"));

      var items = documents.completion(completion).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

      assertThat(items.getRight().getItems())
          .extracting(CompletionItem::getLabel, CompletionItem::getDetail)
          .containsExactly(tuple("interval", "10 sec"));
      documents.didChange(
          new DidChangeTextDocumentParams(
              new VersionedTextDocumentIdentifier(uri, 2),
              List.of(new TextDocumentContentChangeEvent(SHARED_SCRIPT))));
      var hover =
          TestLanguageServers.hover(
              server, uri, 4, SHARED_SCRIPT.lines().toList().get(4).indexOf("{{interval") + 3);
      assertThat(hover.getContents().getRight().getValue()).isEqualTo("`interval` = `10 sec`");
    } finally {
      TestLanguageServers.stop(server, exitCode);
    }
  }
}
