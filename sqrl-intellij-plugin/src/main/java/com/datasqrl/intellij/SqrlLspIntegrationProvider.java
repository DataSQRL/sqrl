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

import com.intellij.openapi.project.Project;
import com.intellij.openapi.project.ProjectManager;
import com.intellij.openapi.vfs.VirtualFile;
import com.intellij.platform.lsp.api.LspClient;
import com.intellij.platform.lsp.api.LspClientManager;
import com.intellij.platform.lsp.api.LspIntegrationProvider;
import com.intellij.platform.lsp.api.lsWidget.LspClientWidgetItem;

/** Starts the SQRL language server when a {@code .sqrl} script or package file is opened. */
public final class SqrlLspIntegrationProvider implements LspIntegrationProvider {

  @Override
  public void fileOpened(Project project, VirtualFile file, LspClientStarter clientStarter) {
    if (SqrlLspClientDescriptor.isSqrl(file) || SqrlLspClientDescriptor.isDataSqrlPackage(file)) {
      clientStarter.ensureClientStarted(new SqrlLspClientDescriptor(project));
    }
  }

  @Override
  public LspClientWidgetItem createWidgetItem(LspClient lspClient, VirtualFile currentFile) {
    return new SqrlWidgetItem(lspClient, currentFile);
  }

  /** Restarts the language servers of all open projects, e.g. after the settings changed. */
  static void restartServers() {
    for (var project : ProjectManager.getInstance().getOpenProjects()) {
      LspClientManager.getInstance(project)
          .stopAndRestartClientsIfNeeded(SqrlLspIntegrationProvider.class);
    }
  }
}
