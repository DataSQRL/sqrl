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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.intellij.icons.AllIcons;
import com.intellij.openapi.actionSystem.ActionGroup;
import com.intellij.openapi.actionSystem.ActionUpdateThread;
import com.intellij.openapi.actionSystem.AnAction;
import com.intellij.openapi.actionSystem.AnActionEvent;
import com.intellij.openapi.actionSystem.DefaultActionGroup;
import com.intellij.openapi.actionSystem.KeepPopupOnPerform;
import com.intellij.openapi.actionSystem.Separator;
import com.intellij.openapi.actionSystem.ToggleAction;
import com.intellij.openapi.actionSystem.ex.ActionUtil;
import com.intellij.openapi.diagnostic.Logger;
import com.intellij.openapi.project.DumbAware;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.vfs.VirtualFile;
import com.intellij.platform.lsp.api.LspClient;
import com.intellij.platform.lsp.api.lsWidget.LspClientWidgetItem;
import com.intellij.ui.LayeredIcon;
import com.intellij.ui.NewUI;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import javax.swing.Icon;
import org.eclipse.lsp4j.ExecuteCommandParams;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The DataSQRL item of the language services widget in the status bar. For a script, the package
 * files that name the script as their main script are listed under it, to select the one that the
 * script is checked with, and the overlays of that one, to add any of them.
 */
final class SqrlWidgetItem extends LspClientWidgetItem {

  private static final Logger LOG = Logger.getInstance(SqrlWidgetItem.class);

  /** The command of the language server that lists the package files of a script */
  static final String PACKAGE_FILES_COMMAND = "sqrl.packageFiles";

  private static final int TIMEOUT_MS = 5_000;

  private final VirtualFile currentFile;

  SqrlWidgetItem(LspClient client, VirtualFile currentFile) {
    super(client, currentFile, SqrlIcons.SQRL, SqrlConfigurable.class);
    this.currentFile = currentFile;
  }

  @Override
  protected @NotNull AnAction createWidgetMainAction() {
    var server = super.createWidgetMainAction();
    if (currentFile == null || !SqrlLspClientDescriptor.isSqrl(currentFile)) {
      return server;
    }
    // The widget lists the actions of a group that is not a popup in place, so the package files
    // are listed under the server, which keeps its buttons, e.g. to restart it
    var presentation = server.getTemplatePresentation();
    if (isError() && presentation.getIcon() != null) {
      presentation.setIcon(
          LayeredIcon.layeredIcon(new Icon[] {presentation.getIcon(), AllIcons.Nodes.ErrorMark}));
    }
    if (NewUI.isEnabled()) {
      presentation.putClientProperty(ActionUtil.INLINE_ACTIONS, createWidgetInlineActions());
    }
    var actions = new DefaultActionGroup();
    actions.add(server);
    actions.add(new PackageFiles(getLspClient(), currentFile));
    return actions;
  }

  /**
   * The package files of a script, which the language server lists, since it finds the package
   * files of scripts and knows which one it checks a script with.
   */
  static final class PackageFiles extends ActionGroup implements DumbAware {

    private final LspClient client;
    private final VirtualFile script;

    PackageFiles(LspClient client, VirtualFile script) {
      this.client = client;
      this.script = script;
    }

    @Override
    public @NotNull ActionUpdateThread getActionUpdateThread() {
      return ActionUpdateThread.BGT;
    }

    @Override
    public AnAction @NotNull [] getChildren(AnActionEvent event) {
      var scriptUri = client.getDocumentIdentifier(script).getUri();
      var packageFiles = request(scriptUri);
      var choices =
          new Choices(
              scriptUri,
              uris(packageFiles, "candidates"),
              uris(packageFiles, "overlays"),
              uris(packageFiles, "used"));
      if (choices.bases().isEmpty()) {
        return AnAction.EMPTY_ARRAY; // e.g. a script that is imported
      }
      var actions = new ArrayList<AnAction>();
      actions.add(Separator.create("Package Files of " + script.getName()));
      choices.bases().forEach(base -> actions.add(new SelectPackageFile(choices, base)));
      choices.overlays().forEach(overlay -> actions.add(new SelectOverlay(choices, overlay)));
      return actions.toArray(AnAction[]::new);
    }

    private JsonObject request(String scriptUri) {
      try {
        Object result =
            client.sendRequestSync(
                TIMEOUT_MS,
                server ->
                    server
                        .getWorkspaceService()
                        .executeCommand(
                            new ExecuteCommandParams(PACKAGE_FILES_COMMAND, List.of(scriptUri))));
        var json =
            result instanceof JsonElement element
                ? element
                : JsonParser.parseString(new Gson().toJson(result));
        return json.isJsonObject() ? json.getAsJsonObject() : new JsonObject();
      } catch (RuntimeException e) {
        LOG.info("Could not list the package files of " + scriptUri, e);
        return new JsonObject();
      }
    }

    private static List<String> uris(JsonObject packageFiles, String name) {
      var uris = new ArrayList<String>();
      if (packageFiles.get(name) != null && packageFiles.get(name).isJsonArray()) {
        packageFiles.getAsJsonArray(name).forEach(uri -> uris.add(uri.getAsString()));
      }
      return uris;
    }
  }

  /**
   * The package files that a script can be checked with.
   *
   * @param script the URI of the script
   * @param bases the URIs of the package files that name the script as their main script
   * @param overlays the URIs of the overlays of the base in use
   * @param used the URIs of the package files that the language server checks the script with
   */
  record Choices(String script, List<String> bases, List<String> overlays, List<String> used) {

    /**
     * The selected package files, the base first, which are stored before the server checks the
     * script with them, or else the ones in use.
     */
    List<String> current(@Nullable Project project) {
      var selected =
          project == null
              ? List.<String>of()
              : SqrlPackageSelection.getInstance(project).selected(script);
      return !selected.isEmpty() && bases.contains(selected.get(0)) ? selected : used;
    }

    void select(@Nullable Project project, List<String> packageFiles) {
      if (project != null) {
        SqrlPackageSelection.getInstance(project).select(script, packageFiles);
      }
    }
  }

  /** Selects a base package file of a script, which is checked when it is the one in use. */
  static final class SelectPackageFile extends ToggleAction implements DumbAware {

    private final Choices choices;
    private final String packageFile;

    SelectPackageFile(Choices choices, String packageFile) {
      // File names often contain `_`, which would otherwise mark a mnemonic
      getTemplatePresentation().setText(fileName(packageFile), false);
      getTemplatePresentation().setKeepPopupOnPerform(KeepPopupOnPerform.Never);
      this.choices = choices;
      this.packageFile = packageFile;
    }

    @Override
    public @NotNull ActionUpdateThread getActionUpdateThread() {
      return ActionUpdateThread.BGT;
    }

    @Override
    public boolean isSelected(@NotNull AnActionEvent event) {
      var current = choices.current(event.getProject());
      return !current.isEmpty() && current.get(0).equals(packageFile);
    }

    @Override
    public void setSelected(@NotNull AnActionEvent event, boolean state) {
      if (state && !isSelected(event)) {
        // Another base, whose overlays the user selects after it
        choices.select(event.getProject(), List.of(packageFile));
      }
    }
  }

  /** Adds an overlay to the base package file of a script, or removes it. */
  static final class SelectOverlay extends ToggleAction implements DumbAware {

    private final Choices choices;
    private final String overlay;

    SelectOverlay(Choices choices, String overlay) {
      getTemplatePresentation().setText("+ " + fileName(overlay), false);
      getTemplatePresentation().setKeepPopupOnPerform(KeepPopupOnPerform.Never);
      this.choices = choices;
      this.overlay = overlay;
    }

    @Override
    public @NotNull ActionUpdateThread getActionUpdateThread() {
      return ActionUpdateThread.BGT;
    }

    @Override
    public boolean isSelected(@NotNull AnActionEvent event) {
      return choices.current(event.getProject()).contains(overlay);
    }

    @Override
    public void setSelected(@NotNull AnActionEvent event, boolean state) {
      var current = choices.current(event.getProject());
      if (current.isEmpty()) {
        return;
      }
      // The server applies the overlays in their order, from the general to the specific
      var packageFiles = new ArrayList<>(current);
      packageFiles.remove(overlay);
      if (state) {
        packageFiles.add(overlay);
      }
      choices.select(event.getProject(), packageFiles);
    }
  }

  private static String fileName(String uri) {
    try {
      return Path.of(URI.create(uri)).getFileName().toString();
    } catch (RuntimeException e) {
      return uri.substring(uri.lastIndexOf('/') + 1);
    }
  }
}
