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

import static com.datasqrl.packager.preprocess.JBangJars.JBANG_JAR_NAME;
import static com.datasqrl.packager.preprocess.JBangJars.isJBangJar;
import static com.datasqrl.packager.preprocess.JBangJars.libJarName;
import static com.datasqrl.packager.preprocess.JBangJars.udfDigests;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.FilePreprocessingPipeline;
import com.datasqrl.util.DirectoryUtils;
import com.datasqrl.util.FilenameAnalyzer;
import com.datasqrl.util.JBangRunner;
import com.google.common.hash.Hashing;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.ExecuteException;
import org.springframework.stereotype.Component;

/**
 * Builds the JBang UDF scripts of a preprocessing run, i.e. of the project or of an included
 * project, into one fat JAR, and registers each function next to its script.
 *
 * <p>The JAR is cached as {@value JBangJars#JBANG_JAR_NAME} in the deepest directory that contains
 * all the scripts, and reused as long as it matches them. Every {@value JBangJars#JBANG_JAR_NAME}
 * is registered by this preprocessor instead of the {@link JarPreprocessor}.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class JBangPreprocessor extends UdfManifestPreprocessor {

  private static final FilenameAnalyzer JBANG_FILES = FilenameAnalyzer.of("java");

  private static final Pattern PACKAGE_PATTERN = Pattern.compile("package\\s+([\\w.]+);");
  private static final Pattern CLASS_EXTENDS_PATTERN =
      Pattern.compile("public\\s+class\\s+(\\w+)\\s+extends\\s+(\\w+)", Pattern.DOTALL);
  private static final String JBANG_SHEBANG = "///usr/bin/env jbang \"$0\" \"$@\" ; exit $?";

  // State of the current pipeline run, reset by complete()
  private final Map<JBangFileInfo, FilePreprocessingPipeline.Context> sources =
      new LinkedHashMap<>();
  private final Map<Path, FilePreprocessingPipeline.Context> jBangJars = new LinkedHashMap<>();

  private final JBangRunner jBangRunner;
  private final ErrorCollector errorCollector;

  @Override
  public void process(Path file, FilePreprocessingPipeline.Context ctx) {
    if (isJBangJar(file)) {
      jBangJars.put(file, ctx);
      return;
    }

    if (!jBangRunner.isJBangAvailable() || !isJBangFile(file)) {
      return;
    }

    var content = readFileContent(file);

    var udfClass = parseUdfClass(file, content);
    if (udfClass == null) {
      return;
    }

    sources.put(udfClass, ctx);
  }

  @Override
  public void complete() {
    try {
      if (sources.isEmpty()) {
        // Nothing to build, e.g. prebuilt JARs only, so register them as plain UDF JARs
        jBangJars.forEach(
            (jar, ctx) -> registerUdfJar(jar, libJarName(ctx.includeNamespace()), ctx));
      } else {
        buildOrReuseJar();
      }
    } finally {
      // The same instance serves every pipeline run: included projects first, then the project
      sources.clear();
      jBangJars.clear();
    }
  }

  private void buildOrReuseJar() {
    var fileInfos =
        sources.keySet().stream().sorted(Comparator.comparing(JBangFileInfo::file)).toList();
    var ctx = sources.values().iterator().next(); // all sources share the lib dir and namespace
    var libJarName = libJarName(ctx.includeNamespace());
    var cachedJar = cachedJar(fileInfos);

    if (isJBangJarExistsAnValid(cachedJar, fileInfos)) {
      errorCollector.notice(
          "Skip building JBang UDFs, as a valid JBang JAR is already present."
              + " To rebuild, delete the '%s' file from your project.",
          cachedJar);
      ctx.copyToLib(cachedJar, libJarName);
    } else {
      buildJar(fileInfos, ctx.libDir().resolve(libJarName), cachedJar);
    }

    sources.forEach(
        (fileInfo, sourceCtx) -> createUdfManifest(fileInfo.udfClassName(), libJarName, sourceCtx));
  }

  /** Caches in the deepest directory of all scripts, as it does not depend on the walk order. */
  private static Path cachedJar(List<JBangFileInfo> fileInfos) {
    var files = fileInfos.stream().map(fileInfo -> fileInfo.file().toAbsolutePath()).toList();
    return DirectoryUtils.deepestCommonSubDir(files).resolve(JBANG_JAR_NAME);
  }

  private void buildJar(List<JBangFileInfo> fileInfos, Path libJar, Path cachedJar) {
    var classNames = fileInfos.stream().map(JBangFileInfo::udfClassName).toList();
    try {
      jBangRunner.exportFatJar(new ArrayDeque<>(fileInfos), libJar);

    } catch (ExecuteException e) {
      throw errorCollector.exception(
          "Failed to build JBang UDF(s) %s. %s", classNames, e.getMessage());

    } catch (IOException e) {
      throw errorCollector.exception(
          "Failed to execute JBang export for UDF(s) %s: %s", classNames, e.getMessage());
    }

    try {
      Files.copy(libJar, cachedJar, StandardCopyOption.REPLACE_EXISTING);
    } catch (Exception e) {
      log.warn("Failed to copy JBang JAR to source directory", e);
    }
  }

  @SneakyThrows
  private boolean isJBangFile(Path file) {
    if (JBANG_FILES.analyze(file).isEmpty()) {
      return false;
    }

    var firstLine = Files.readAllLines(file).get(0).trim();
    return JBANG_SHEBANG.equals(firstLine);
  }

  private boolean isJBangJarExistsAnValid(Path jbangJar, List<JBangFileInfo> jBangFiles) {
    if (!Files.isRegularFile(jbangJar)) {
      return false;
    }

    try {
      var udfDigests = udfDigests(jbangJar);
      return jBangFiles.stream()
          .allMatch(
              jBangFile ->
                  jBangFile.sha256Digest().equals(udfDigests.get(jBangFile.udfClassName())));

    } catch (IOException e) {
      log.warn("Failed to read JBang file hashes from JBang JAR '{}', rebuilding...", jbangJar);
      return false;
    }
  }

  @SneakyThrows
  private String readFileContent(Path file) {
    return Files.readString(file);
  }

  private JBangFileInfo parseUdfClass(Path file, String content) {
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

    var extendedClass = classMatcherRes.group(2); // group 2 is the extended class

    // Match against both canonical and simple class name
    var extendedUdfClass =
        FLINK_UDFS.stream()
            .filter(
                udfParentClass ->
                    udfParentClass.equals(extendedClass) || udfParentClass.endsWith(extendedClass))
            .findFirst();

    if (extendedUdfClass.isEmpty()) {
      log.warn(
          "Skip preprocessing file {}, as it does not extend a proper Flink UDF parent class",
          file);
      return null;
    }

    var className = new StringJoiner(".");

    // Extract package (optional in JBang files)
    var packageMatcher = PACKAGE_PATTERN.matcher(content);
    if (packageMatcher.find()) {
      className.add(packageMatcher.group(1));
    }

    className.add(classMatcherRes.group(1)); // group 1 is the class name

    var sha256Digest = Hashing.sha256().hashString(content, StandardCharsets.UTF_8).toString();

    return new JBangFileInfo(file, className.toString(), extendedUdfClass.get(), sha256Digest);
  }

  public record JBangFileInfo(
      Path file, String udfClassName, String parentUdfClassName, String sha256Digest) {}
}
