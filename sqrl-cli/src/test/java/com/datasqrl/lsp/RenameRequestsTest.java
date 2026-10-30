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
import static com.datasqrl.lsp.TestLanguageServers.awaitHover;
import static com.datasqrl.lsp.TestLanguageServers.hover;
import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.compile.check.CheckTestProject;
import com.datasqrl.lsp.TestLanguageServers.RecordingClient;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.lsp4j.ClientCapabilities;
import org.eclipse.lsp4j.DidChangeTextDocumentParams;
import org.eclipse.lsp4j.DidOpenTextDocumentParams;
import org.eclipse.lsp4j.InitializeResult;
import org.eclipse.lsp4j.Position;
import org.eclipse.lsp4j.RenameParams;
import org.eclipse.lsp4j.TextDocumentContentChangeEvent;
import org.eclipse.lsp4j.TextDocumentIdentifier;
import org.eclipse.lsp4j.TextDocumentItem;
import org.eclipse.lsp4j.TextEdit;
import org.eclipse.lsp4j.VersionedTextDocumentIdentifier;
import org.eclipse.lsp4j.services.LanguageServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Renames through the language server while the editor changes the script. */
class RenameRequestsTest {

  @TempDir Path tempDir;

  /**
   * IntelliJ previews the new name in the script while it is typed, which the server checks, and
   * restores the name before it asks for the rename.
   */
  @Test
  void givenScriptChangedSinceTheLastCheck_whenRename_thenRenamesAfterCheckingItAgain()
      throws Exception {
    var project = CheckTestProject.create(tempDir);
    var uri = project.mainScript().toUri().toString();
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
      documents.didOpen(
          new DidOpenTextDocumentParams(new TextDocumentItem(uri, "sqrl", 1, MAIN_SCRIPT)));
      awaitHover(server, uri, 2, 0);
      documents.didChange(change(uri, 2, MAIN_SCRIPT.replace("OrderStream :=", "OrderEvents :=")));
      awaitRenamedInCheck(server, uri);
      documents.didChange(change(uri, 3, MAIN_SCRIPT));

      var edit =
          documents
              .rename(
                  new RenameParams(
                      new TextDocumentIdentifier(uri), new Position(2, 0), "OrderEvents"))
              .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

      assertThat(edit.getChanges().get(uri))
          .extracting(TextEdit::getNewText)
          .containsExactly("OrderEvents", "OrderEvents");
    } finally {
      TestLanguageServers.stop(server, exitCode);
    }
  }

  private static DidChangeTextDocumentParams change(String uri, int version, String text) {
    return new DidChangeTextDocumentParams(
        new VersionedTextDocumentIdentifier(uri, version),
        List.of(new TextDocumentContentChangeEvent(text)));
  }

  /** Waits until the last check has the new name of the table that is defined on line 3. */
  private static void awaitRenamedInCheck(LanguageServer server, String uri) throws Exception {
    var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
    while (System.nanoTime() < deadline) {
      var hover = hover(server, uri, 2, 0);
      if (hover != null && !hover.getContents().getRight().getValue().contains("OrderStream")) {
        return;
      }
      Thread.sleep(100);
    }
    throw new AssertionError("The script with the new name was not checked");
  }
}
