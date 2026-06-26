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
package com.datasqrl.loaders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.planner.parser.StatementParserException;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ModuleLoadersTest {

  @Mock private ModuleLoader mainLoader;
  @Mock private ModuleLoader rootLoader;
  @Mock private SqrlModule module;

  @Test
  void givenRegularImport_whenLoadImportModule_thenUsesMainLoaderAndPreservesPath() {
    var moduleLoaders = new ModuleLoaders(mainLoader, rootLoader, Set.of("shared"));
    when(mainLoader.loadModule(NamePath.of("submodule"))).thenReturn(Optional.of(module));

    var loadedModule =
        moduleLoaders.loadImportModule(NamePath.of("submodule", "Table"), FileLocation.START);

    assertThat(loadedModule.module()).isSameAs(module);
    assertThat(loadedModule.finalPath()).isEqualTo(NamePath.of("submodule", "Table"));
    assertThat(loadedModule.rootImport()).isFalse();
    verify(mainLoader).loadModule(NamePath.of("submodule"));
    verifyNoInteractions(rootLoader);
  }

  @Test
  void givenRootImport_whenLoadImportModule_thenUsesRootLoaderAndRemovesRootPrefix() {
    var moduleLoaders = new ModuleLoaders(mainLoader, rootLoader, Set.of("shared"));
    when(rootLoader.loadModule(NamePath.of("shared"))).thenReturn(Optional.of(module));

    var loadedModule =
        moduleLoaders.loadImportModule(NamePath.of("root", "shared", "Table"), FileLocation.START);

    assertThat(loadedModule.module()).isSameAs(module);
    assertThat(loadedModule.finalPath()).isEqualTo(NamePath.of("shared", "Table"));
    assertThat(loadedModule.rootImport()).isTrue();
    verify(rootLoader).loadModule(NamePath.of("shared"));
    verifyNoInteractions(mainLoader);
  }

  @Test
  void
      givenMissingSharedScriptImportWithoutRoot_whenLoadImportModule_thenReportsRootPrefixRequirement() {
    var moduleLoaders = new ModuleLoaders(mainLoader, rootLoader, Set.of("shared"));
    when(mainLoader.loadModule(NamePath.of("shared"))).thenReturn(Optional.empty());

    assertThatThrownBy(
            () ->
                moduleLoaders.loadImportModule(NamePath.of("shared", "Table"), FileLocation.START))
        .isInstanceOf(StatementParserException.class)
        .hasMessage(
            "Invalid import, to access a shared script in a submodule make sure to use the 'root' prefix");
    verify(mainLoader).loadModule(NamePath.of("shared"));
    verifyNoInteractions(rootLoader);
  }

  @Test
  void
      givenMissingSharedScriptExportWithoutRoot_whenLoadExportModule_thenReportsRootPrefixRequirement() {
    var moduleLoaders = new ModuleLoaders(mainLoader, rootLoader, Set.of("shared"));
    when(mainLoader.loadModule(NamePath.of("shared"))).thenReturn(Optional.empty());

    assertThatThrownBy(
            () ->
                moduleLoaders.loadExportModule(NamePath.of("shared", "Table"), FileLocation.START))
        .isInstanceOf(StatementParserException.class)
        .hasMessage(
            "Invalid export, to access a shared script in a submodule make sure to use the 'root' prefix");
    verify(mainLoader).loadModule(NamePath.of("shared"));
    verifyNoInteractions(rootLoader);
  }

  @Test
  void givenMissingRegularImport_whenLoadImportModule_thenReportsMissingModule() {
    var moduleLoaders = new ModuleLoaders(mainLoader, rootLoader, Set.of("shared"));
    when(mainLoader.loadModule(NamePath.of("missing"))).thenReturn(Optional.empty());

    assertThatThrownBy(
            () ->
                moduleLoaders.loadImportModule(NamePath.of("missing", "Table"), FileLocation.START))
        .isInstanceOf(StatementParserException.class)
        .hasMessage("Could not find module [missing.Table] at path: [missing/Table]");
    verify(mainLoader).loadModule(NamePath.of("missing"));
    verifyNoInteractions(rootLoader);
  }

  @Test
  void givenMainLoaderReplacement_whenLoadImportModule_thenUsesReplacementAndKeepsRootLoader() {
    var replacementLoader = mock(ModuleLoader.class);
    var moduleLoaders =
        new ModuleLoaders(mainLoader, rootLoader, Set.of("shared"))
            .withMainLoader(replacementLoader);
    when(replacementLoader.loadModule(NamePath.of("local"))).thenReturn(Optional.of(module));
    when(rootLoader.loadModule(NamePath.of("shared"))).thenReturn(Optional.of(module));

    assertThat(
            moduleLoaders
                .loadImportModule(NamePath.of("local", "Table"), FileLocation.START)
                .module())
        .isSameAs(module);
    assertThat(
            moduleLoaders
                .loadImportModule(NamePath.of("root", "shared", "Table"), FileLocation.START)
                .module())
        .isSameAs(module);

    verify(replacementLoader).loadModule(NamePath.of("local"));
    verify(rootLoader).loadModule(NamePath.of("shared"));
    verify(mainLoader, never()).loadModule(NamePath.of("local"));
  }

  @Test
  void givenNamePath_whenCheckRootImport_thenOnlyRootHeadMatches() {
    var moduleLoaders = new ModuleLoaders(mainLoader, rootLoader, Set.of());

    assertThat(moduleLoaders.isRootImport(NamePath.of("root", "shared"))).isTrue();
    assertThat(moduleLoaders.isRootImport(NamePath.of("shared", "root"))).isFalse();
    assertThat(moduleLoaders.isRootImport(NamePath.ROOT)).isFalse();
  }
}
