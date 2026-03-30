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
package com.datasqrl.util;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class UdfCompiler {

  private static final String JBANG_SHEBANG = "///usr/bin/env jbang";
  private static final Set<String> SIGNATURE_ENTRIES = Set.of("META-INF/MANIFEST.MF");
  private static final String SIGNATURE_PREFIX = "META-INF/";

  private final MavenDependencyResolver dependencyResolver;

  public static UdfCompiler create(MavenDependencyResolver resolver) {
    return new UdfCompiler(resolver);
  }

  public static UdfCompiler withClasspath(String classpath, MavenDependencyResolver resolver) {
    return new UdfCompiler(resolver) {
      @Override
      String resolveClasspath() {
        return classpath;
      }
    };
  }

  public static UdfCompiler disabled() {
    return new DisabledCompiler();
  }

  public void compileAndPackage(List<Path> sourceFiles, Path targetJar) throws IOException {
    var allJdeps = new ArrayList<String>();
    var processedSources = new ArrayList<SourceFile>();

    for (var srcFile : sourceFiles) {
      var content = Files.readString(srcFile);
      allJdeps.addAll(dependencyResolver.parseJdeps(content));
      processedSources.add(new SourceFile(srcFile, stripShebang(content)));
    }

    var resolvedDeps = dependencyResolver.resolve(allJdeps);

    var classpath = buildClasspath(resolvedDeps);

    var classOutputDir = Files.createTempDirectory("sqrl-udf-classes");
    try {
      compile(processedSources, classpath, classOutputDir);
      packageJar(classOutputDir, resolvedDeps, targetJar);
    } finally {
      deleteRecursive(classOutputDir);
    }
  }

  private void compile(List<SourceFile> sources, String classpath, Path outputDir)
      throws IOException {
    var compiler = ToolProvider.getSystemJavaCompiler();
    if (compiler == null) {
      throw new IllegalStateException(
          "Java compiler not available. UDF compilation requires a JDK (not JRE).");
    }

    var diagnostics = new DiagnosticCollector<JavaFileObject>();
    var fileManager = compiler.getStandardFileManager(diagnostics, null, null);

    var compilationUnits =
        sources.stream()
            .map(src -> new InMemoryJavaSource(src.file.getFileName().toString(), src.content))
            .collect(Collectors.toList());

    var options = new ArrayList<String>();
    options.add("-classpath");
    options.add(classpath);
    options.add("-d");
    options.add(outputDir.toString());
    options.add("--release");
    options.add("17");

    var task = compiler.getTask(null, fileManager, diagnostics, options, null, compilationUnits);
    var success = task.call();

    fileManager.close();

    if (!success) {
      var errors = new StringBuilder("UDF compilation failed:\n");
      diagnostics
          .getDiagnostics()
          .forEach(
              d ->
                  errors
                      .append("  ")
                      .append(d.getKind())
                      .append(": ")
                      .append(d.getMessage(null))
                      .append(" (line ")
                      .append(d.getLineNumber())
                      .append(")\n"));
      throw new IOException(errors.toString());
    }

    log.debug("Successfully compiled {} UDF source files", sources.size());
  }

  private void packageJar(Path classOutputDir, List<Path> depJars, Path targetJar)
      throws IOException {
    Files.createDirectories(targetJar.getParent());
    var addedEntries = new HashSet<String>();

    try (var out = new JarOutputStream(Files.newOutputStream(targetJar))) {
      // Add compiled .class files
      Files.walkFileTree(
          classOutputDir,
          new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
              var entryName =
                  classOutputDir.relativize(file).toString().replace(File.separatorChar, '/');
              if (addedEntries.add(entryName)) {
                out.putNextEntry(new JarEntry(entryName));
                Files.copy(file, out);
                out.closeEntry();
              }
              return FileVisitResult.CONTINUE;
            }
          });

      // Add dependency JAR entries
      for (var depJar : depJars) {
        if (!Files.exists(depJar) || !depJar.toString().endsWith(".jar")) {
          continue;
        }
        try (var jar = new JarFile(depJar.toFile())) {
          var entries = jar.entries();
          while (entries.hasMoreElements()) {
            var entry = entries.nextElement();
            if (isSignatureFile(entry.getName())) {
              continue;
            }
            if (entry.isDirectory()) {
              continue;
            }
            if (addedEntries.add(entry.getName())) {
              out.putNextEntry(new JarEntry(entry.getName()));
              try (var is = jar.getInputStream(entry)) {
                is.transferTo(out);
              }
              out.closeEntry();
            }
          }
        }
      }
    }

    log.debug("Packaged UDF JAR: {} ({} bytes)", targetJar, Files.size(targetJar));
  }

  private static boolean isSignatureFile(String entryName) {
    if (SIGNATURE_ENTRIES.contains(entryName)) {
      return true;
    }
    if (entryName.startsWith(SIGNATURE_PREFIX)) {
      var name = entryName.substring(SIGNATURE_PREFIX.length());
      if (!name.contains("/")) {
        return name.endsWith(".SF") || name.endsWith(".DSA") || name.endsWith(".RSA");
      }
    }
    return false;
  }

  String resolveClasspath() {
    return resolveCliJarPath()
        .map(Path::toString)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Cannot resolve sqrl-cli.jar path. "
                        + "UDF compilation requires the CLI fat JAR on the classpath to provide Flink dependencies."));
  }

  private String buildClasspath(List<Path> resolvedDeps) {
    var parts = new ArrayList<String>();
    parts.add(resolveClasspath());
    resolvedDeps.forEach(p -> parts.add(p.toString()));
    return String.join(File.pathSeparator, parts);
  }

  static Optional<Path> resolveCliJarPath() {
    try {
      var codeSource = UdfCompiler.class.getProtectionDomain().getCodeSource();
      if (codeSource == null) {
        return Optional.empty();
      }

      var location = Path.of(codeSource.getLocation().toURI());
      if (!location.toString().endsWith(".jar")) {
        return Optional.empty();
      }

      return Optional.of(location);
    } catch (URISyntaxException e) {
      log.debug("Failed to resolve CLI jar path", e);
      return Optional.empty();
    }
  }

  static String stripShebang(String content) {
    if (content.startsWith(JBANG_SHEBANG)) {
      var newlineIdx = content.indexOf('\n');
      if (newlineIdx >= 0) {
        return content.substring(newlineIdx + 1);
      }
    }
    return content;
  }

  private static void deleteRecursive(Path dir) {
    try {
      Files.walkFileTree(
          dir,
          new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
              Files.delete(file);
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path d, IOException exc) throws IOException {
              Files.delete(d);
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (IOException e) {
      log.debug("Failed to clean up temp directory: {}", dir, e);
    }
  }

  private record SourceFile(Path file, String content) {}

  private static class InMemoryJavaSource extends SimpleJavaFileObject {

    private final String code;

    InMemoryJavaSource(String fileName, String code) {
      super(java.net.URI.create("string:///" + fileName), Kind.SOURCE);
      this.code = code;
    }

    @Override
    public CharSequence getCharContent(boolean ignoreEncodingErrors) {
      return code;
    }
  }

  private static class DisabledCompiler extends UdfCompiler {

    DisabledCompiler() {
      super(null);
    }

    @Override
    public void compileAndPackage(List<Path> sourceFiles, Path targetJar) {
      // do nothing
    }
  }
}
