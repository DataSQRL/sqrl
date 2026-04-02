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

import com.datasqrl.packager.FilePreprocessingPipeline;
import com.datasqrl.util.MavenDependencyResolver;
import com.datasqrl.util.UdfCompiler;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class JavaUdfPreprocessor extends UdfManifestPreprocessor {

  private static final Pattern PACKAGE_PATTERN = Pattern.compile("package\\s+([\\w.]+);");
  private static final Pattern CLASS_EXTENDS_PATTERN =
      Pattern.compile("public\\s+class\\s+(\\w+)\\s+extends\\s+(\\w+)", Pattern.DOTALL);

  static final String UDF_JAR_NAME = "sqrl-udfs.jar";

  private final UdfCompiler udfCompiler;
  private final MavenDependencyResolver dependencyResolver;
  private final List<UdfFileInfo> collectedFiles = new ArrayList<>();
  private FilePreprocessingPipeline.Context ctx;

  @Override
  public void process(Path file, FilePreprocessingPipeline.Context ctx) {
    if (!isJavaFile(file)) {
      return;
    }

    var content = readFileContent(file);

    dependencyResolver.validateNoFlinkJdeps(file, content);

    var udfClassName = parseUdfClassName(file, content);
    if (udfClassName == null) {
      return;
    }

    this.ctx = ctx;
    collectedFiles.add(new UdfFileInfo(file, udfClassName));
  }

  @Override
  public void complete() {
    if (collectedFiles.isEmpty()) {
      return;
    }

    var targetPath = ctx.libDir().resolve(UDF_JAR_NAME);
    var allPaths = collectedFiles.stream().map(UdfFileInfo::file).toList();
    var allClassNames = collectedFiles.stream().map(UdfFileInfo::udfClassName).toList();

    try {
      udfCompiler.compileAndPackage(allPaths, targetPath);
      createUdfManifests(allClassNames, UDF_JAR_NAME, ctx);
    } catch (IOException e) {
      log.warn("UDF compilation failed", e);
    }
  }

  private record UdfFileInfo(Path file, String udfClassName) {}

  private boolean isJavaFile(Path file) {
    return file.getFileName().toString().endsWith(".java");
  }

  @SneakyThrows
  private String readFileContent(Path file) {
    return Files.readString(file);
  }

  private String parseUdfClassName(Path file, String content) {
    var classExtendsMatcher = CLASS_EXTENDS_PATTERN.matcher(content);
    var results = classExtendsMatcher.results().toList();
    if (results.isEmpty()) {
      log.info(
          "Skip preprocessing file {}, as it does not contain a 'public class' with an 'extends' statement",
          file);
      return null;
    }

    if (results.size() > 1) {
      log.warn(
          "Skip preprocessing file {}, as it contains multiple public classes that are extending another class",
          file);
      return null;
    }

    var classMatcherRes = results.get(0);
    var extendedClass = classMatcherRes.group(2);

    var extendsUdfClass =
        FLINK_UDFS.stream()
            .anyMatch(
                udfParentClass ->
                    udfParentClass.equals(extendedClass) || udfParentClass.endsWith(extendedClass));

    if (!extendsUdfClass) {
      log.warn(
          "Skip preprocessing file {}, as it does not extend a proper Flink UDF parent class",
          file);
      return null;
    }

    var packageMatcher = PACKAGE_PATTERN.matcher(content);
    var packageName = packageMatcher.find() ? packageMatcher.group(1) + "." : "";

    var className = classMatcherRes.group(1);

    return packageName + className;
  }
}
