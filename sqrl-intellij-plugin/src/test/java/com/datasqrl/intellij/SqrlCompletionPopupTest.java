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
package com.datasqrl.intellij;

import com.intellij.codeInsight.lookup.LookupElement;
import com.intellij.codeInsight.lookup.LookupElementPresentation;
import com.intellij.openapi.command.WriteCommandAction;
import com.intellij.platform.lsp.api.LspClientManager;
import com.intellij.testFramework.EdtTestUtil;
import com.intellij.testFramework.UsefulTestCase;
import com.intellij.testFramework.fixtures.CodeInsightTestFixture;
import com.intellij.testFramework.fixtures.CompletionAutoPopupTester;
import com.intellij.testFramework.fixtures.IdeaTestFixtureFactory;
import com.intellij.testFramework.fixtures.impl.TempDirTestFixtureImpl;
import java.util.concurrent.TimeUnit;

/**
 * Types in a script that the language server checked, as the user does, which shows completions
 * without asking for them. Skipped if the jar of the CLI was not built, see {@link
 * SqrlTestProject}.
 */
public class SqrlCompletionPopupTest extends UsefulTestCase {

  private CodeInsightTestFixture fixture;
  private SqrlSettings.Options previousSettings;

  /** Typing waits for the popup, which the IDE shows on its event thread */
  @Override
  protected boolean runInDispatchThread() {
    return false;
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    var factory = IdeaTestFixtureFactory.getFixtureFactory();
    fixture =
        factory.createCodeInsightFixture(
            factory.createFixtureBuilder(getName()).getFixture(), new TempDirTestFixtureImpl());
    fixture.setUp();
    var settings = SqrlSettings.getInstance();
    previousSettings = settings.getState();
    settings.loadState(new SqrlSettings.Options());
  }

  @Override
  protected void tearDown() throws Exception {
    try {
      LspClientManager.getInstance(fixture.getProject())
          .stopClients(SqrlLspIntegrationProvider.class);
      SqrlSettings.getInstance().loadState(previousSettings);
      fixture.tearDown();
    } catch (Throwable e) {
      addSuppressedException(e);
    } finally {
      super.tearDown();
    }
  }

  public void testTablesAreSuggestedWhileTyping() throws Throwable {
    if (!SqrlTestProject.useCliJar()) {
      return;
    }
    EdtTestUtil.runInEdtAndWait(
        () -> fixture.configureFromExistingVirtualFile(SqrlTestProject.create(fixture)));
    awaitCheck();
    EdtTestUtil.runInEdtAndWait(
        () -> {
          var document = fixture.getEditor().getDocument();
          WriteCommandAction.runWriteCommandAction(
              fixture.getProject(),
              () -> document.insertString(document.getTextLength(), "X := SELECT * FROM "));
          fixture.getEditor().getCaretModel().moveToOffset(document.getTextLength());
        });
    var tester = new CompletionAutoPopupTester(fixture);

    tester.runWithAutoPopupEnabled(
        () -> {
          tester.typeWithPauses("Ord");

          assertNotNull("No completions were shown", tester.getLookup());
          // The tables of the language server, with their type, not the words of the file
          var tables =
              tester.getLookup().getItems().stream()
                  .map(item -> item.getLookupString() + ": " + typeText(item))
                  .toList();
          assertContainsElements(tables, "OrderStream: stream", "Orders: versioned state");
        });
  }

  public void testHintsAreSuggestedWhileTyping() throws Throwable {
    if (!SqrlTestProject.useCliJar()) {
      return;
    }
    EdtTestUtil.runInEdtAndWait(
        () -> fixture.configureFromExistingVirtualFile(SqrlTestProject.create(fixture)));
    awaitCheck();
    EdtTestUtil.runInEdtAndWait(
        () -> {
          var document = fixture.getEditor().getDocument();
          WriteCommandAction.runWriteCommandAction(
              fixture.getProject(), () -> document.insertString(document.getTextLength(), "\n"));
          fixture.getEditor().getCaretModel().moveToOffset(document.getTextLength());
        });
    var tester = new CompletionAutoPopupTester(fixture);

    tester.runWithAutoPopupEnabled(
        () -> {
          tester.typeWithPauses("/*+query");

          assertNotNull("No hints were shown", tester.getLookup());
          assertContainsElements(
              tester.getLookup().getItems().stream().map(LookupElement::getLookupString).toList(),
              "query_by_all",
              "query_by_any");
        });
  }

  public void testColumnsOfHintsAreSuggestedWhileTyping() throws Throwable {
    if (!SqrlTestProject.useCliJar()) {
      return;
    }
    EdtTestUtil.runInEdtAndWait(
        () -> fixture.configureFromExistingVirtualFile(SqrlTestProject.create(fixture)));
    awaitCheck();
    // Before the definition of Orders
    EdtTestUtil.runInEdtAndWait(
        () ->
            fixture
                .getEditor()
                .getCaretModel()
                .moveToOffset(SqrlTestProject.SCRIPT.indexOf("Orders :=")));
    var tester = new CompletionAutoPopupTester(fixture);

    tester.runWithAutoPopupEnabled(
        () -> {
          tester.typeWithPauses("/*+query_by_all(");

          assertNotNull("No columns were shown", tester.getLookup());
          assertContainsElements(
              tester.getLookup().getItems().stream().map(LookupElement::getLookupString).toList(),
              "id",
              "customer_id");
        });
  }

  public void testVariablesOfTemplatesAreSuggestedWhileTyping() throws Throwable {
    if (!SqrlTestProject.useCliJar()) {
      return;
    }
    EdtTestUtil.runInEdtAndWait(
        () -> fixture.configureFromExistingVirtualFile(SqrlTestProject.create(fixture)));
    awaitCheck();
    EdtTestUtil.runInEdtAndWait(
        () -> {
          var document = fixture.getEditor().getDocument();
          WriteCommandAction.runWriteCommandAction(
              fixture.getProject(),
              () -> document.insertString(document.getTextLength(), "IMPORT connectors.police_"));
          fixture.getEditor().getCaretModel().moveToOffset(document.getTextLength());
        });
    var tester = new CompletionAutoPopupTester(fixture);

    tester.runWithAutoPopupEnabled(
        () -> {
          tester.typeWithPauses("{{");

          assertNotNull("No variables were shown", tester.getLookup());
          var variables =
              tester.getLookup().getItems().stream()
                  .map(item -> item.getLookupString() + ": " + typeText(item))
                  .toList();
          assertContainsElements(variables, "variant: test");
        });
  }

  private static String typeText(LookupElement item) {
    return EdtTestUtil.runInEdtAndGet(
        () -> LookupElementPresentation.renderElement(item).getTypeText());
  }

  private void awaitCheck() throws InterruptedException {
    var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(SqrlTestProject.TIMEOUT_SECONDS);
    while (System.nanoTime() < deadline) {
      if (EdtTestUtil.runInEdtAndGet(() -> SqrlTestProject.error(fixture)).isPresent()) {
        return;
      }
      Thread.sleep(200);
    }
    throw new AssertionError("The language server did not check the script");
  }
}
