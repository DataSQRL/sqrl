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

import com.intellij.openapi.components.PersistentStateComponent;
import com.intellij.openapi.components.Service;
import com.intellij.openapi.components.State;
import com.intellij.openapi.components.Storage;
import com.intellij.openapi.components.StoragePathMacros;
import com.intellij.openapi.project.Project;
import com.intellij.platform.lsp.api.LspClientManager;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import kotlin.Unit;
import org.eclipse.lsp4j.DidChangeConfigurationParams;

/**
 * The package files that the user selected for the scripts of a project, when several package files
 * name a script as their main script, e.g. one for local runs and one for tests. They are stored
 * with the workspace of the project, and passed to the language server as its setting {@code
 * selectedPackageFiles}.
 */
@Service(Service.Level.PROJECT)
@State(name = "DataSqrlPackageFiles", storages = @Storage(StoragePathMacros.WORKSPACE_FILE))
public final class SqrlPackageSelection
    implements PersistentStateComponent<SqrlPackageSelection.Selections> {

  /** The selections, which are public fields to be stored by the IDE. */
  public static final class Selections {
    /**
     * The URIs of the selected package files of each script, one per line, by the URI of the
     * script: the base first, then its overlays
     */
    public Map<String, String> packageFiles = new TreeMap<>();
  }

  private final Project project;
  private Selections selections = new Selections();

  public SqrlPackageSelection(Project project) {
    this.project = project;
  }

  public static SqrlPackageSelection getInstance(Project project) {
    return project.getService(SqrlPackageSelection.class);
  }

  @Override
  public synchronized Selections getState() {
    return selections;
  }

  @Override
  public synchronized void loadState(Selections state) {
    this.selections = state;
  }

  /** The URIs of the package files that are selected for a script, the base first, if any. */
  synchronized List<String> selected(String script) {
    var files = selections.packageFiles.get(script);
    return files == null ? List.of() : List.of(files.split("\n"));
  }

  /** The settings of the language server with the selections. */
  synchronized Map<String, Object> settings() {
    var selected = new TreeMap<String, List<String>>();
    selections.packageFiles.keySet().forEach(script -> selected.put(script, selected(script)));
    return Map.of("selectedPackageFiles", selected);
  }

  /**
   * Selects the package files of a script, and lets the running language servers check it with
   * them.
   *
   * @param script the URI of the script
   * @param packageFiles the URIs of a base and its overlays, or empty for the ones that are found
   *     by default
   */
  void select(String script, List<String> packageFiles) {
    synchronized (this) {
      if (packageFiles.isEmpty()) {
        selections.packageFiles.remove(script);
      } else {
        selections.packageFiles.put(script, String.join("\n", packageFiles));
      }
    }
    var changed = new DidChangeConfigurationParams(settings());
    for (var client :
        LspClientManager.getInstance(project).getClients(SqrlLspIntegrationProvider.class)) {
      client.sendNotification(
          server -> {
            server.getWorkspaceService().didChangeConfiguration(changed);
            return Unit.INSTANCE;
          });
    }
  }
}
