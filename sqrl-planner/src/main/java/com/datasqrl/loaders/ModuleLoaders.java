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
import com.datasqrl.config.PackageJson.IncludeConfig;
import com.datasqrl.config.WorkspacePaths;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.loaders.resolver.FileResourceResolver;
import com.datasqrl.loaders.resolver.ResourceResolver;
import com.datasqrl.planner.parser.SqrlStatementParser;
import com.datasqrl.util.SqrlObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import jakarta.inject.Inject;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@Getter
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public final class ModuleLoaders {

  static final String ROOT_PREFIX = "root";

  private final ModuleLoader mainLoader;
  private final ModuleLoader rootLoader;
  private final Set<String> includeNamespaces;
  private final Map<String, Path> standaloneIncludePaths;
  // How the deployment of each standalone include plans its scripts, analyzed on first use
  private final Map<String, Supplier<IncludeDeployment>> includeDeployments;

  @Inject
  public ModuleLoaders(
      PackageJson packageJson,
      ResourceResolver resourceResolver,
      SqrlStatementParser sqrlStatementParser,
      WorkspacePaths workspacePaths,
      ClasspathFunctionLoader classpathFunctionLoader,
      UdfJarClassLoaders udfJarClassLoaders,
      ErrorCollector errors) {

    mainLoader =
        new ModuleLoaderImpl(
            resourceResolver,
            sqrlStatementParser,
            workspacePaths,
            classpathFunctionLoader,
            udfJarClassLoaders,
            errors);
    rootLoader =
        new ModuleLoaderImpl(
            new FileResourceResolver(workspacePaths.buildDir()),
            sqrlStatementParser,
            workspacePaths,
            classpathFunctionLoader,
            udfJarClassLoaders,
            errors);
    var scriptConfig = packageJson.getScriptConfig();
    includeNamespaces =
        scriptConfig.getIncludeConfigs().stream()
            .map(IncludeConfig::getNamespace)
            .collect(Collectors.toSet());
    standaloneIncludePaths =
        scriptConfig.getStandaloneIncludes().stream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    namespace ->
                        workspacePaths.getIncludePath(namespace).toAbsolutePath().normalize()));
    includeDeployments =
        scriptConfig.getIncludeConfigs().stream()
            .filter(IncludeConfig::isStandalone)
            .collect(
                Collectors.toMap(
                    IncludeConfig::getNamespace,
                    include ->
                        Suppliers.memoize(
                            () ->
                                analyzeDeployment(
                                    include,
                                    standaloneIncludePaths.get(include.getNamespace()),
                                    includePath ->
                                        new ModuleLoaderImpl(
                                            new FileResourceResolver(includePath),
                                            sqrlStatementParser,
                                            workspacePaths,
                                            classpathFunctionLoader,
                                            udfJarClassLoaders,
                                            // The included project reports its own errors
                                            ErrorCollector.root()),
                                    sqrlStatementParser,
                                    errors))));
  }

  @VisibleForTesting
  ModuleLoaders(
      ModuleLoader mainLoader,
      ModuleLoader rootLoader,
      Set<String> includeNamespaces,
      Map<String, Path> standaloneIncludePaths) {
    this(mainLoader, rootLoader, includeNamespaces, standaloneIncludePaths, Map.of());
  }

  public ModuleLoaders withMainLoader(ModuleLoader mainLoader) {
    return new ModuleLoaders(
        mainLoader, rootLoader, includeNamespaces, standaloneIncludePaths, includeDeployments);
  }

  public Optional<String> getStandaloneInclude(Path scriptPath) {
    var path = scriptPath.toAbsolutePath().normalize();
    return standaloneIncludePaths.entrySet().stream()
        .filter(include -> path.startsWith(include.getValue()))
        .map(Map.Entry::getKey)
        .findFirst();
  }

  /**
   * Returns the standalone include that a script belongs to, with how that include's own deployment
   * plans the script, or empty if the project owns the script.
   */
  public Optional<IncludedScript> getIncludedScript(Path scriptPath) {
    return getStandaloneInclude(scriptPath)
        .map(
            include ->
                new IncludedScript(
                    include,
                    Suppliers.memoize(
                        () ->
                            includeDeployments
                                .getOrDefault(include, () -> IncludeDeployment.NONE)
                                .get()
                                .getPlanning(scriptPath))));
  }

  /**
   * Analyzes which scripts the deployment of a standalone include plans, starting from the main
   * script that the include's package file sets. Without one, the deployment plans no script.
   */
  private static IncludeDeployment analyzeDeployment(
      IncludeConfig include,
      Path includePath,
      Function<Path, ModuleLoader> includeLoader,
      SqrlStatementParser parser,
      ErrorCollector errors) {

    var packageFile = includePath.resolve(Path.of(include.getPackage()).getFileName().toString());
    var mainScript = readMainScript(packageFile);
    if (mainScript.isEmpty()) {
      errors.warn(
          "Include '%s' sets \"standalone\": true, but its package file '%s' sets no"
              + " \"script.main\". Without it, this project cannot tell which statements of '%1$s'"
              + " its deployment runs, so this project runs them itself, e.g. it writes the topics"
              + " of their subscriptions.",
          include.getNamespace(), include.getPackage());

      return IncludeDeployment.NONE;
    }

    return IncludeDeployment.analyze(includeLoader.apply(includePath), mainScript.get(), parser);
  }

  private static Optional<String> readMainScript(Path packageFile) {
    try {
      var main =
          SqrlObjectMapper.INSTANCE.readTree(packageFile.toFile()).path("script").path("main");
      return main.isTextual() ? Optional.of(main.asText()) : Optional.empty();
    } catch (IOException e) {
      return Optional.empty();
    }
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
        ctx.module != null || ctx.rootImport || !includeNamespaces.contains(pathHead),
        fileLocation,
        export ? ErrorCode.INVALID_EXPORT : ErrorCode.INVALID_IMPORT,
        "Invalid %s, to access an included script in a submodule make sure to use the '%s' prefix",
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
