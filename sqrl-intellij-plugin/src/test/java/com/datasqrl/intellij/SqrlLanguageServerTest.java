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

import com.intellij.codeInsight.daemon.impl.HighlightInfo;
import com.intellij.codeInsight.template.impl.TemplateManagerImpl;
import com.intellij.lang.annotation.HighlightSeverity;
import com.intellij.openapi.actionSystem.IdeActions;
import com.intellij.openapi.application.WriteAction;
import com.intellij.openapi.command.CommandProcessor;
import com.intellij.openapi.command.WriteCommandAction;
import com.intellij.openapi.editor.DefaultLanguageHighlighterColors;
import com.intellij.openapi.editor.colors.TextAttributesKey;
import com.intellij.openapi.vfs.VfsUtil;
import com.intellij.platform.lsp.api.LspClientManager;
import com.intellij.testFramework.PlatformTestUtil;
import com.intellij.testFramework.UsefulTestCase;
import com.intellij.testFramework.fixtures.CodeInsightTestFixture;
import com.intellij.testFramework.fixtures.IdeaTestFixtureFactory;
import com.intellij.testFramework.fixtures.impl.CodeInsightTestFixtureImpl;
import com.intellij.testFramework.fixtures.impl.TempDirTestFixtureImpl;
import com.jetbrains.jsonSchema.impl.inspections.JsonSchemaComplianceInspection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Starts the language server from the jar of the CLI, which the build passes as {@code
 * sqrl.cli.jar}, for a project on disk, and checks that its errors and fixes show in the editor.
 * Skipped if the jar was not built.
 */
public class SqrlLanguageServerTest extends UsefulTestCase {

  private CodeInsightTestFixture fixture;
  private SqrlSettings.Options previousSettings;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    SqrlPackageSchemaTest.allowSchemaAccess(getTestRootDisposable());
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

  public void testErrorsOfTheCompilerAreHighlightedAndFixed() throws Exception {
    var error = openCheckedScript();
    if (error == null) {
      return;
    }
    var text = fixture.getEditor().getDocument().getText();
    assertEquals("amout", text.substring(error.getStartOffset(), error.getEndOffset()));

    fixture.getEditor().getCaretModel().moveToOffset(error.getStartOffset());
    fixture.launchAction(fixture.findSingleIntention("Change to `amount`"));

    assertEquals(
        SqrlTestProject.SCRIPT.replace("SUM(amout)", "SUM(amount)"),
        fixture.getEditor().getDocument().getText());
  }

  public void testMessagesWithTypesAreShownInTooltips() throws Exception {
    if (openCheckedScript() == null) {
      return;
    }
    var document = fixture.getEditor().getDocument();
    WriteCommandAction.runWriteCommandAction(
        fixture.getProject(),
        () ->
            document.insertString(
                document.getTextLength(),
                "Late := SELECT * FROM OrderStream WHERE updated_at > 50;"));

    var error =
        await(
            () ->
                CodeInsightTestFixtureImpl.instantiateAndRun(
                        fixture.getFile(), fixture.getEditor(), new int[0], true)
                    .stream()
                    .filter(
                        info ->
                            info.getDescription() != null
                                && info.getDescription().startsWith("Cannot apply '>'"))
                    .findFirst());

    assertTrue(error.getToolTip(), error.getToolTip().contains("&lt;INTEGER&gt;"));
  }

  public void testTablesAreCompleted() throws Exception {
    if (openCheckedScript() == null) {
      return;
    }
    var document = fixture.getEditor().getDocument();
    WriteCommandAction.runWriteCommandAction(
        fixture.getProject(),
        () -> document.insertString(document.getTextLength(), "X := SELECT * FROM Ord"));
    fixture.getEditor().getCaretModel().moveToOffset(document.getTextLength());

    fixture.completeBasic();

    assertContainsElements(fixture.getLookupElementStrings(), "OrderStream", "Orders");
  }

  public void testTablesAreRenamed() throws Exception {
    if (openCheckedScript() == null) {
      return;
    }
    var editor = fixture.getEditor();
    editor.getCaretModel().moveToOffset(SqrlTestProject.SCRIPT.indexOf("OrderStream :="));
    TemplateManagerImpl.setTemplateTesting(fixture.getTestRootDisposable());

    // The new name is typed in the editor, then the language server renames it everywhere
    fixture.performEditorAction(IdeActions.ACTION_RENAME);
    var template = TemplateManagerImpl.getTemplateState(editor);
    assertNotNull(template);
    var name = template.getCurrentVariableRange();
    WriteCommandAction.runWriteCommandAction(
        fixture.getProject(),
        () ->
            editor
                .getDocument()
                .replaceString(name.getStartOffset(), name.getEndOffset(), "OrderEvents"));
    // As if Enter was pressed
    CommandProcessor.getInstance()
        .executeCommand(fixture.getProject(), () -> template.gotoEnd(false), "Rename", null);

    var expected = SqrlTestProject.SCRIPT.replace("OrderStream", "OrderEvents");
    await(() -> Optional.of(editor.getDocument().getText()).filter(text -> text.equals(expected)));
  }

  public void testNamesAreColoredByWhatTheyAre() throws Exception {
    if (openCheckedScript() == null) {
      return;
    }
    // Of statements without errors
    var table = SqrlTestProject.SCRIPT.indexOf("OrderStream");
    var column = SqrlTestProject.SCRIPT.indexOf("updated_at");

    var colored =
        await(
            () -> {
              var infos =
                  CodeInsightTestFixtureImpl.instantiateAndRun(
                      fixture.getFile(), fixture.getEditor(), new int[0], true);
              var tableKey = attributesAt(infos, table);
              var columnKey = attributesAt(infos, column);
              return tableKey.isPresent() && columnKey.isPresent()
                  ? Optional.of(List.of(tableKey.get(), columnKey.get()))
                  : Optional.empty();
            });

    assertEquals(
        List.of(
            DefaultLanguageHighlighterColors.CLASS_NAME,
            DefaultLanguageHighlighterColors.INSTANCE_FIELD),
        colored);
  }

  public void testTypesOfComputedColumnsAreShownInline() throws Exception {
    if (openCheckedScript() == null) {
      return;
    }
    var document = fixture.getEditor().getDocument();
    var fixed = SqrlTestProject.SCRIPT.replace("SUM(amout)", "SUM(amount)");
    WriteCommandAction.runWriteCommandAction(fixture.getProject(), () -> document.setText(fixed));
    var afterTotal = fixed.indexOf("AS total") + "AS total".length();

    // The server answers once it checked the fixed script
    var hint =
        await(
            () -> {
              CodeInsightTestFixtureImpl.instantiateAndRun(
                  fixture.getFile(), fixture.getEditor(), new int[0], true);
              return fixture
                  .getEditor()
                  .getInlayModel()
                  .getInlineElementsInRange(afterTotal, afterTotal)
                  .stream()
                  .map(inlay -> inlay.getRenderer().toString())
                  .filter(text -> text.contains("DOUBLE"))
                  .findFirst();
            });

    assertTrue(hint, hint.contains(": DOUBLE"));
  }

  public void testErrorsOfPackageFilesAreHighlightedAtTheirValue() throws Exception {
    if (!SqrlTestProject.useCliJar()) {
      return;
    }
    SqrlTestProject.create(fixture);
    var packageFile = fixture.getTempDirFixture().getFile("project/package.json");
    WriteAction.runAndWait(
        () ->
            VfsUtil.saveText(
                packageFile,
                VfsUtil.loadText(packageFile).replace("\"flink\"]", "\"flink\", \"nope\"]")));
    fixture.enableInspections(JsonSchemaComplianceInspection.class);
    fixture.configureFromExistingVirtualFile(packageFile);

    var error =
        await(
            () ->
                CodeInsightTestFixtureImpl.instantiateAndRun(
                        fixture.getFile(), fixture.getEditor(), new int[0], true)
                    .stream()
                    .filter(info -> info.getSeverity() == HighlightSeverity.ERROR)
                    .filter(info -> info.getDescription().contains("/enabled-engines/4"))
                    .findFirst());

    var text = fixture.getEditor().getDocument().getText();
    assertEquals("\"nope\"", text.substring(error.getStartOffset(), error.getEndOffset()));
    // The JSON schema of package files reports it as well
    var highlights =
        CodeInsightTestFixtureImpl.instantiateAndRun(
            fixture.getFile(), fixture.getEditor(), new int[0], true);
    assertTrue(
        highlights.toString(),
        highlights.stream()
            .anyMatch(
                info ->
                    info.getSeverity() == HighlightSeverity.WARNING
                        && "\"nope\"".equals(info.getText())));
  }

  /** The colors of a name that the language server highlighted, at its start. */
  private static Optional<TextAttributesKey> attributesAt(List<HighlightInfo> infos, int offset) {
    return infos.stream()
        .filter(info -> info.getStartOffset() == offset && info.forcedTextAttributesKey != null)
        .map(info -> info.forcedTextAttributesKey)
        .findFirst();
  }

  /**
   * Opens the main script of the test project, and waits until the language server checked it.
   *
   * @return the error in the script, or null if the test is skipped since the jar of the CLI was
   *     not built
   */
  private HighlightInfo openCheckedScript() throws Exception {
    if (!SqrlTestProject.useCliJar()) {
      return null;
    }
    fixture.configureFromExistingVirtualFile(SqrlTestProject.create(fixture));
    return await(() -> SqrlTestProject.error(fixture));
  }

  /** Waits for a result while the IDE handles its events, e.g. the diagnostics of the server. */
  private static <T> T await(Supplier<Optional<T>> result) throws InterruptedException {
    var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(SqrlTestProject.TIMEOUT_SECONDS);
    while (System.nanoTime() < deadline) {
      PlatformTestUtil.dispatchAllEventsInIdeEventQueue();
      var found = result.get();
      if (found.isPresent()) {
        return found.get();
      }
      Thread.sleep(200);
    }
    throw new AssertionError("The language server did not respond as expected");
  }
}
