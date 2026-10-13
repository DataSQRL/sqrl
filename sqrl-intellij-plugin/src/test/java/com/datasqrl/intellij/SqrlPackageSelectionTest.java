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

import com.datasqrl.intellij.SqrlWidgetItem.PackageFiles;
import com.datasqrl.intellij.SqrlWidgetItem.SelectOverlay;
import com.datasqrl.intellij.SqrlWidgetItem.SelectPackageFile;
import com.intellij.codeInsight.daemon.impl.HighlightInfo;
import com.intellij.lang.annotation.HighlightSeverity;
import com.intellij.openapi.actionSystem.ActionGroup;
import com.intellij.openapi.actionSystem.AnAction;
import com.intellij.openapi.actionSystem.AnActionEvent;
import com.intellij.openapi.actionSystem.DefaultActionGroup;
import com.intellij.openapi.actionSystem.KeepPopupOnPerform;
import com.intellij.openapi.actionSystem.Separator;
import com.intellij.openapi.actionSystem.ToggleAction;
import com.intellij.openapi.actionSystem.impl.SimpleDataContext;
import com.intellij.openapi.application.ApplicationManager;
import com.intellij.openapi.ui.popup.JBPopupFactory;
import com.intellij.openapi.ui.popup.ListPopupStep;
import com.intellij.openapi.util.Disposer;
import com.intellij.platform.lsp.api.LspClientManager;
import com.intellij.testFramework.PlatformTestUtil;
import com.intellij.testFramework.TestActionEvent;
import com.intellij.testFramework.UsefulTestCase;
import com.intellij.testFramework.fixtures.CodeInsightTestFixture;
import com.intellij.testFramework.fixtures.IdeaTestFixtureFactory;
import com.intellij.testFramework.fixtures.impl.CodeInsightTestFixtureImpl;
import com.intellij.testFramework.fixtures.impl.TempDirTestFixtureImpl;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Selects the package file of a script in the DataSQRL item of the language services widget, with
 * the language server from the jar of the CLI. Skipped if the jar was not built.
 */
public class SqrlPackageSelectionTest extends UsefulTestCase {

  private CodeInsightTestFixture fixture;
  private SqrlSettings.Options previousSettings;

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

  public void testPackageFileOfScriptIsSelectedInTheWidget() throws Exception {
    if (!SqrlTestProject.useCliJar()) {
      return;
    }
    var main = SqrlTestProject.create(fixture);
    // Without the package that the script imports
    var other =
        fixture
            .getTempDirFixture()
            .createFile(
                "project/main_package_other.json",
                "{\"version\": \"1\", \"script\": {\"main\": \"main.sqrl\"}}");
    fixture.configureFromExistingVirtualFile(main);
    await(() -> SqrlTestProject.error(fixture));
    var project = fixture.getProject();
    var client =
        LspClientManager.getInstance(project)
            .getClients(SqrlLspIntegrationProvider.class)
            .iterator()
            .next();

    var actions = (ActionGroup) new SqrlWidgetItem(client, main).createWidgetAction();
    var event = TestActionEvent.createTestEvent(SimpleDataContext.getProjectContext(project));
    var packageFiles = (PackageFiles) actions.getChildren(event)[1];
    var children = childrenInBackground(packageFiles, event);

    assertEquals("Package Files of main.sqrl", ((Separator) children.get(0)).getText());
    var choices =
        children.stream()
            .skip(1)
            .map(action -> (SelectPackageFile) action)
            .map(action -> action.getTemplateText() + "=" + action.isSelected(event))
            .toList();
    assertEquals(List.of("main_package_other.json=false", "package.json=true"), choices);

    var selectOther = (SelectPackageFile) children.get(1);
    assertEquals(
        KeepPopupOnPerform.Never, selectOther.getTemplatePresentation().getKeepPopupOnPerform());
    selectOther.setSelected(event, true);
    // The tick moves at once, before the server checked the script with the other file
    assertTrue(selectOther.isSelected(event));
    assertFalse(((SelectPackageFile) children.get(2)).isSelected(event));

    var mainUri = client.getDocumentIdentifier(main).getUri();
    var otherUri = client.getDocumentIdentifier(other).getUri();
    assertEquals(
        Map.of(mainUri, otherUri),
        SqrlPackageSelection.getInstance(project).getState().packageFiles);
    assertEquals(
        Map.of("selectedPackageFiles", Map.of(mainUri, List.of(otherUri))),
        new SqrlLspClientDescriptor(project).createInitializationOptions());
    // The import fails with the other package file
    var importEnd = SqrlTestProject.SCRIPT.indexOf('\n');
    await(
        () ->
            CodeInsightTestFixtureImpl.instantiateAndRun(
                    fixture.getFile(), fixture.getEditor(), new int[0], true)
                .stream()
                .filter(info -> info.getSeverity() == HighlightSeverity.ERROR)
                .filter(info -> info.getStartOffset() < importEnd)
                .map(HighlightInfo::getDescription)
                .findFirst());
  }

  public void testWidgetPopupListsThePackageFilesUnderTheServer() throws Exception {
    if (!SqrlTestProject.useCliJar()) {
      return;
    }
    var main = SqrlTestProject.create(fixture);
    fixture
        .getTempDirFixture()
        .createFile(
            "project/main_package_other.json",
            "{\"version\": \"1\", \"script\": {\"main\": \"main.sqrl\"}}");
    fixture.configureFromExistingVirtualFile(main);
    await(() -> SqrlTestProject.error(fixture));
    var project = fixture.getProject();
    var client =
        LspClientManager.getInstance(project)
            .getClients(SqrlLspIntegrationProvider.class)
            .iterator()
            .next();

    // As the language services widget builds its popup
    var widget = new DefaultActionGroup();
    widget.add(new SqrlWidgetItem(client, main).createWidgetAction());
    var popup =
        JBPopupFactory.getInstance()
            .createActionGroupPopup(
                "Language Services",
                widget,
                SimpleDataContext.getProjectContext(project),
                JBPopupFactory.ActionSelectionAid.SPEEDSEARCH,
                false);
    Disposer.register(fixture.getTestRootDisposable(), popup);
    @SuppressWarnings("unchecked")
    var step = (ListPopupStep<Object>) popup.getListStep();
    var values = step.getValues();

    assertEquals(
        List.of("DataSQRL", "main_package_other.json", "package.json"),
        values.stream().map(step::getTextFor).map(text -> text.split(" ")[0]).toList());
    assertFalse(step.hasSubstep(values.get(0)));
    assertEquals("Package Files of main.sqrl", step.getSeparatorAbove(values.get(1)).getText());
  }

  public void testOverlaysOfTheBaseAreListedAndToggled() throws Exception {
    if (!SqrlTestProject.useCliJar()) {
      return;
    }
    SqrlTestProject.create(fixture);
    // A base without the package that the script imports, which its overlay for tests includes
    var files = fixture.getTempDirFixture();
    files.createFile(
        "split/package.json",
        "{\"version\": \"1\", \"enabled-engines\": [\"vertx\", \"postgres\", \"kafka\","
            + " \"flink\"], \"script\": {\"main\": \"main.sqrl\"}}");
    files.createFile(
        "split/package-test.json",
        "{\"script\": {\"include\": {\"shop\": {\"package\": \"../shared/package.json\"}}}}");
    files.createFile("split/package-deploy.json", "{\"script\": {\"config\": {}}}");
    var main = files.createFile("split/main.sqrl", SqrlTestProject.SCRIPT);
    fixture.configureFromExistingVirtualFile(main);
    // Checked with the overlay for tests, so the import works and the unknown column is reported
    await(() -> SqrlTestProject.error(fixture));
    var project = fixture.getProject();
    var client =
        LspClientManager.getInstance(project)
            .getClients(SqrlLspIntegrationProvider.class)
            .iterator()
            .next();

    var actions = (ActionGroup) new SqrlWidgetItem(client, main).createWidgetAction();
    var event = TestActionEvent.createTestEvent(SimpleDataContext.getProjectContext(project));
    var children = childrenInBackground((ActionGroup) actions.getChildren(event)[1], event);

    assertEquals(
        List.of("package.json=true", "+ package-test.json=true", "+ package-deploy.json=false"),
        children.stream()
            .skip(1)
            .map(action -> (ToggleAction) action)
            .map(action -> action.getTemplateText() + "=" + action.isSelected(event))
            .toList());

    ((SelectOverlay) children.get(2)).setSelected(event, false);

    var mainUri = client.getDocumentIdentifier(main).getUri();
    assertEquals(
        List.of(client.getDocumentIdentifier(main.getParent().findChild("package.json")).getUri()),
        SqrlPackageSelection.getInstance(project).selected(mainUri));
    // Without the overlay, the import fails
    var importEnd = SqrlTestProject.SCRIPT.indexOf('\n');
    await(
        () ->
            CodeInsightTestFixtureImpl.instantiateAndRun(
                    fixture.getFile(), fixture.getEditor(), new int[0], true)
                .stream()
                .filter(info -> info.getSeverity() == HighlightSeverity.ERROR)
                .filter(info -> info.getStartOffset() < importEnd)
                .map(HighlightInfo::getDescription)
                .findFirst());
  }

  /** The children of a group that asks the language server, which must not block the UI. */
  private static List<AnAction> childrenInBackground(ActionGroup group, AnActionEvent event)
      throws Exception {
    var children =
        ApplicationManager.getApplication().executeOnPooledThread(() -> group.getChildren(event));
    var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(SqrlTestProject.TIMEOUT_SECONDS);
    while (!children.isDone() && System.nanoTime() < deadline) {
      PlatformTestUtil.dispatchAllEventsInIdeEventQueue();
      Thread.sleep(50);
    }
    return Arrays.asList(children.get(1, TimeUnit.SECONDS));
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
