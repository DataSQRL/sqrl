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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonParser;
import java.nio.channels.Channels;
import java.nio.channels.Pipe;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.eclipse.lsp4j.ClientCapabilities;
import org.eclipse.lsp4j.Diagnostic;
import org.eclipse.lsp4j.Hover;
import org.eclipse.lsp4j.HoverParams;
import org.eclipse.lsp4j.InitializeParams;
import org.eclipse.lsp4j.InitializeResult;
import org.eclipse.lsp4j.InitializedParams;
import org.eclipse.lsp4j.MessageActionItem;
import org.eclipse.lsp4j.MessageParams;
import org.eclipse.lsp4j.Position;
import org.eclipse.lsp4j.PublishDiagnosticsParams;
import org.eclipse.lsp4j.ShowMessageRequestParams;
import org.eclipse.lsp4j.TextDocumentIdentifier;
import org.eclipse.lsp4j.launch.LSPLauncher;
import org.eclipse.lsp4j.services.LanguageClient;
import org.eclipse.lsp4j.services.LanguageServer;

/** Runs language servers in-process for tests, connected to clients over the LSP protocol. */
final class TestLanguageServers {

  static final long TIMEOUT_SECONDS = 120;

  private TestLanguageServers() {}

  /** Starts a server and connects a client to it, which initializes it with the capabilities. */
  static LanguageServer start(
      Path workDir,
      LanguageClient languageClient,
      ClientCapabilities capabilities,
      AtomicInteger exitCode,
      InitializeResult[] result)
      throws Exception {
    // Unlike piped streams, NIO pipes do not break when a writing thread ends
    var clientToServer = Pipe.open();
    var serverToClient = Pipe.open();
    var languageServer = new SqrlLanguageServer(workDir, exitCode::set);
    var serverLauncher =
        new LSPLauncher.Builder<LanguageClient>()
            .setLocalService(languageServer)
            .setRemoteInterface(LanguageClient.class)
            .setInput(Channels.newInputStream(clientToServer.source()))
            .setOutput(Channels.newOutputStream(serverToClient.sink()))
            .configureGson(SqrlLanguageServer::configureGson)
            .create();
    languageServer.connect(serverLauncher.getRemoteProxy());
    serverLauncher.startListening();

    var clientLauncher =
        new LSPLauncher.Builder<LanguageServer>()
            .setLocalService(languageClient)
            .setRemoteInterface(LanguageServer.class)
            .setInput(Channels.newInputStream(serverToClient.source()))
            .setOutput(Channels.newOutputStream(clientToServer.sink()))
            .configureGson(SqrlLanguageServer::configureGson)
            .create();
    clientLauncher.startListening();
    var remoteServer = clientLauncher.getRemoteProxy();

    var init = new InitializeParams();
    init.setCapabilities(capabilities);
    init.setInitializationOptions(JsonParser.parseString("{\"checkDelayMillis\": 10}"));
    result[0] = remoteServer.initialize(init).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    remoteServer.initialized(new InitializedParams());
    return remoteServer;
  }

  static void stop(LanguageServer languageServer, AtomicInteger exitCode) throws Exception {
    languageServer.shutdown().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    languageServer.exit();
    // exit is a notification, so wait until the server handled it
    var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (exitCode.get() < 0 && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertThat(exitCode.get()).isZero();
  }

  static Hover hover(LanguageServer languageServer, String uri, int line, int character)
      throws Exception {
    return languageServer
        .getTextDocumentService()
        .hover(new HoverParams(new TextDocumentIdentifier(uri), new Position(line, character)))
        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  /** Waits until the project has been checked, so that the hover is known. */
  static Hover awaitHover(LanguageServer languageServer, String uri, int line, int character)
      throws Exception {
    var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
    while (System.nanoTime() < deadline) {
      var hover = hover(languageServer, uri, line, character);
      if (hover != null) {
        return hover;
      }
      Thread.sleep(100);
    }
    throw new AssertionError("No hover for " + uri + " at " + line + ":" + character);
  }

  /** Records the published diagnostics, message requests, which it answers, and refreshes. */
  static class RecordingClient implements LanguageClient {

    private final BlockingQueue<PublishDiagnosticsParams> published = new LinkedBlockingQueue<>();
    final List<ShowMessageRequestParams> messageRequests = new CopyOnWriteArrayList<>();
    final AtomicInteger semanticTokensRefreshes = new AtomicInteger();
    final AtomicInteger inlayHintsRefreshes = new AtomicInteger();
    final AtomicInteger codeLensRefreshes = new AtomicInteger();
    private volatile MessageActionItem answer;

    void answer(MessageActionItem action) {
      this.answer = action;
    }

    /** Waits for diagnostics of the document that match the condition. */
    List<Diagnostic> await(String uri, Predicate<List<Diagnostic>> condition)
        throws InterruptedException {
      var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
      while (System.nanoTime() < deadline) {
        var params = published.poll(1, TimeUnit.SECONDS);
        if (params != null
            && params.getUri().equals(uri)
            && condition.test(params.getDiagnostics())) {
          return params.getDiagnostics();
        }
      }
      throw new AssertionError("No matching diagnostics were published for " + uri);
    }

    @Override
    public void publishDiagnostics(PublishDiagnosticsParams diagnostics) {
      published.add(diagnostics);
    }

    @Override
    public void telemetryEvent(Object object) {}

    @Override
    public void showMessage(MessageParams messageParams) {}

    @Override
    public CompletableFuture<MessageActionItem> showMessageRequest(
        ShowMessageRequestParams requestParams) {
      messageRequests.add(requestParams);
      return CompletableFuture.completedFuture(answer);
    }

    @Override
    public void logMessage(MessageParams message) {}

    @Override
    public CompletableFuture<Void> refreshSemanticTokens() {
      semanticTokensRefreshes.incrementAndGet();
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> refreshInlayHints() {
      inlayHintsRefreshes.incrementAndGet();
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> refreshCodeLenses() {
      codeLensRefreshes.incrementAndGet();
      return CompletableFuture.completedFuture(null);
    }
  }
}
