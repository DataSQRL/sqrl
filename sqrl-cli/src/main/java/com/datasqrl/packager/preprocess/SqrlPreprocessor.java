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
package com.datasqrl.packager.preprocess;

import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.FilePreprocessingPipeline;
import com.datasqrl.util.ConfigLoaderUtils;
import com.datasqrl.util.FilenameAnalyzer;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.MustacheFactory;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class SqrlPreprocessor implements Preprocessor {

  private static final FilenameAnalyzer SCRIPT_PATTERN =
      FilenameAnalyzer.of(SqrlConstants.SQRL_EXTENSION);
  private static final MustacheFactory MUSTACHE_FACTORY = new DefaultMustacheFactory();

  private final Map<String, PackageJson> includePackages = new HashMap<>();

  private final PackageJson config;
  private final ErrorCollector errors;

  @Override
  public void process(Path file, FilePreprocessingPipeline.Context ctx) {
    if (SCRIPT_PATTERN.analyze(file).isEmpty()) {
      return;
    }

    var scriptDir = file.getParent();
    var templateValues = createTemplateValues(scriptDir);

    if (templateValues.isEmpty()) {
      ctx.copyToBuild(file);
      return;
    }

    try {
      var content = Files.readString(file);
      var mustache =
          MUSTACHE_FACTORY.compile(new StringReader(content), file.getFileName().toString());

      var writer = new StringWriter();
      mustache.execute(writer, templateValues);

      var target = ctx.createNewBuildFile(file);
      Files.writeString(target, writer.toString());

    } catch (Exception e) {
      throw new IllegalStateException("Failed to resolve template in SQRL file: " + file, e);
    }
  }

  private Map<String, Object> createTemplateValues(Path scriptDir) {
    var includes = config.getScriptConfig().getIncludeConfigs();

    for (var include : includes) {
      var includePackagePathStr = include.getPackage();
      var includePackagePath = Path.of(includePackagePathStr);
      var includeDir = getMatchingSubpath(scriptDir, includePackagePath.getParent().toString());

      if (includeDir.isPresent()) {
        var resolvedIncludePackagePath = includeDir.get().resolve(includePackagePath.getFileName());

        var includePackage =
            includePackages.computeIfAbsent(
                include.getNamespace(), k -> loadIncludePackage(k, resolvedIncludePackagePath));

        var templateValues = new HashMap<>(includePackage.getScriptConfig().getConfig());
        // Apply any overrides
        templateValues.putAll(include.getConfig());

        return templateValues;
      }
    }

    // Not an include script, use the project script config
    return config.getScriptConfig().getConfig();
  }

  private PackageJson loadIncludePackage(String namespace, Path includePackagePath) {
    var localErrors = errors.withInclude(namespace);
    if (!Files.exists(includePackagePath)) {
      localErrors.fatal("Given include package does not exist: %s", includePackagePath);
    }

    return ConfigLoaderUtils.loadResolvedConfigFromFile(localErrors, includePackagePath);
  }

  private static Optional<Path> getMatchingSubpath(Path path, String subpathStr) {
    var subpath = Path.of(subpathStr);
    int subpathLen = subpath.getNameCount();

    for (int i = 0; i <= path.getNameCount() - subpathLen; i++) {
      if (path.subpath(i, i + subpathLen).equals(subpath)) {
        var matchingSubpath = path.subpath(0, i + subpathLen);
        return Optional.of(
            path.isAbsolute() ? path.getRoot().resolve(matchingSubpath) : matchingSubpath);
      }
    }

    return Optional.empty();
  }
}
