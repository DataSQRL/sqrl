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

import static com.datasqrl.planner.parser.StatementParserException.checkFatal;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.SharedScriptConfig;
import com.datasqrl.config.WorkspacePaths;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.loaders.resolver.FileResourceResolver;
import com.datasqrl.loaders.resolver.ResourceResolver;
import jakarta.inject.Inject;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@Getter
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public final class ModuleLoaders {

  private static final String ROOT_PREFIX = "root";

  private final ModuleLoader mainLoader;
  private final ModuleLoader rootLoader;
  private final Set<String> sharedScriptNames;

  @Inject
  public ModuleLoaders(
      PackageJson packageJson,
      ResourceResolver resourceResolver,
      WorkspacePaths workspacePaths,
      ClasspathFunctionLoader classpathFunctionLoader,
      ErrorCollector errors) {

    mainLoader =
        new ModuleLoaderImpl(resourceResolver, workspacePaths, classpathFunctionLoader, errors);
    rootLoader =
        new ModuleLoaderImpl(
            new FileResourceResolver(workspacePaths.buildDir()),
            workspacePaths,
            classpathFunctionLoader,
            errors);
    sharedScriptNames =
        packageJson.getScriptConfig().getSharedScriptConfigs().stream()
            .map(SharedScriptConfig::getName)
            .collect(Collectors.toSet());
  }

  public ModuleLoaders withMainLoader(ModuleLoader mainLoader) {
    return new ModuleLoaders(mainLoader, rootLoader, sharedScriptNames);
  }

  public ModuleContext loadImportModule(NamePath namePath, FileLocation fileLocation) {
    var ctx = doLoad(namePath);
    validate(ctx, namePath, fileLocation, false);

    return ctx;
  }

  public ModuleContext loadExportModule(NamePath namePath, FileLocation fileLocation) {
    var ctx = doLoad(namePath);
    validate(ctx, namePath, fileLocation, true);

    return ctx;
  }

  ModuleContext doLoad(NamePath namePath) {
    var loader = mainLoader;
    var finalPath = namePath;
    boolean rootImport = false;

    if (isRootImport(namePath)) {
      loader = rootLoader;
      finalPath = namePath.popFirst(); // remove the 'root' prefix
      rootImport = true;
    }

    var module = loader.loadModule(finalPath.popLast()).orElse(null);

    return new ModuleContext(module, finalPath, rootImport);
  }

  void validate(ModuleContext ctx, NamePath origPath, FileLocation fileLocation, boolean export) {
    var pathHead = ctx.finalPath.getFirst().getDisplay();

    checkFatal(
        ctx.module != null || ctx.rootImport || !sharedScriptNames.contains(pathHead),
        fileLocation,
        export ? ErrorCode.INVALID_EXPORT : ErrorCode.INVALID_IMPORT,
        "Invalid %s, to access a shared script in a submodule make sure to use the '%s' prefix",
        export ? "export" : "import",
        ROOT_PREFIX);

    checkFatal(
        ctx.module != null,
        fileLocation,
        ErrorLabel.GENERIC,
        "Could not find module [%s] at path: [%s]",
        origPath,
        String.join("/", ctx.finalPath.toStringList()));
  }

  boolean isRootImport(NamePath path) {
    return !path.isEmpty() && ROOT_PREFIX.equals(path.getFirst().getDisplay());
  }

  public record ModuleContext(SqrlModule module, NamePath finalPath, boolean rootImport) {}
}
