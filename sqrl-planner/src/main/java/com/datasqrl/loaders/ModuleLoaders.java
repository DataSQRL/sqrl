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

  private static final String ROOT_IMPORT = "root";

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

  public LoadedModule loadModule(NamePath namePath, FileLocation fileLocation) {
    var loader = mainLoader;
    var finalPath = namePath;
    boolean rootImport = false;

    if (isRootImport(namePath)) {
      loader = rootLoader;
      finalPath = namePath.popFirst(); // remove the 'root' prefix
      rootImport = true;
    }

    var module = loader.loadModule(finalPath.popLast()).orElse(null);
    var importPathHead = finalPath.getFirst().getDisplay();

    checkFatal(
        module != null || rootImport || !sharedScriptNames.contains(importPathHead),
        fileLocation,
        ErrorCode.INVALID_IMPORT,
        "Invalid import, to access a shared script in a submodule make sure to use the '%s' prefix",
        ROOT_IMPORT);

    checkFatal(
        module != null,
        fileLocation,
        ErrorLabel.GENERIC,
        "Could not find module [%s] at path: [%s]",
        namePath,
        String.join("/", finalPath.toStringList()));

    return new LoadedModule(module, finalPath);
  }

  boolean isRootImport(NamePath path) {
    return !path.isEmpty() && ROOT_IMPORT.equals(path.getFirst().getDisplay());
  }

  public record LoadedModule(SqrlModule module, NamePath finalPath) {}
}
