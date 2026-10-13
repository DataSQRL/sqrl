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
package com.datasqrl.lsp;

import com.datasqrl.compile.check.ScriptText;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.lsp4j.Position;

/**
 * Finds the Java sources of user-defined functions that IMPORT statements import, e.g. {@code
 * udfs/src/main/java/com/myudf/read_partition_sizes.java} for {@code IMPORT
 * udfs.read_partition_sizes}. The functions are loaded from jars, or compiled by JBang from sources
 * next to the scripts, so their sources are found by their classes in the project.
 */
@Slf4j
final class FunctionSources {

  /** An IMPORT statement on one line, with the imported path and its alias */
  private static final Pattern IMPORT =
      Pattern.compile(
          "^\\s*IMPORT\\s+(?<path>[\\w.-]+)(?:\\s+AS\\s+(?<alias>\\w+))?",
          Pattern.CASE_INSENSITIVE);

  /** Directories without sources, or with copies of them, e.g. the build directory of DataSQRL */
  private static final Set<String> SKIPPED = Set.of("build", "target", "node_modules");

  private static final int MAX_DEPTH = 16;

  private FunctionSources() {}

  /**
   * What an IMPORT statement imports.
   *
   * @param name the name that it is imported as, e.g. the alias
   * @param path the imported path, e.g. {@code [udfs, read_partition_sizes]}
   */
  record Import(String name, List<String> path) {

    /** The imported directory, e.g. {@code udfs}, relative to the project. */
    Path directory(Path projectDir) {
      return projectDir.resolve(String.join("/", path.subList(0, path.size() - 1)));
    }
  }

  /** What the IMPORT statement at a position imports, if the position is on its path or alias. */
  static Optional<Import> at(String text, int line, int character) {
    var lineText = text.lines().skip(line).findFirst();
    if (lineText.isEmpty()) {
      return Optional.empty();
    }
    var matcher = IMPORT.matcher(lineText.get());
    if (!matcher.find() || character < matcher.start("path") || character > matcher.end()) {
      return Optional.empty();
    }
    var path = List.of(matcher.group("path").split("\\."));
    var alias = matcher.group("alias");
    return Optional.of(new Import(alias != null ? alias : path.get(path.size() - 1), path));
  }

  /** The name of the function that is called at a position, e.g. in {@code fn(1)}. */
  static Optional<String> callAt(String text, int line, int character) {
    var script = new ScriptText(text);
    var offset = script.offset(line, character);
    var name = script.nameAt(offset);
    if (name.isEmpty()) {
      return Optional.empty();
    }
    var parts = name.get();
    var after = parts.get(parts.size() - 1).end();
    while (after < text.length() && Character.isWhitespace(text.charAt(after))) {
      after++;
    }
    return after < text.length() && text.charAt(after) == '('
        ? Optional.of(parts.get(parts.size() - 1).name())
        : Optional.empty();
  }

  /** The IMPORT statement of a script that imports a function with a name, if any. */
  static Optional<Import> importOf(String text, String name) {
    var lines = text.lines().toList();
    for (var line = 0; line < lines.size(); line++) {
      var matcher = IMPORT.matcher(lines.get(line));
      if (matcher.find()) {
        var imported = at(text, line, matcher.start("path"));
        if (imported.isPresent() && imported.get().name().equalsIgnoreCase(name)) {
          return imported;
        }
      }
    }
    return Optional.empty();
  }

  /**
   * The source of a class in a project: a file with the directories of its package, e.g. {@code
   * com/myudf/MyScalarFunction.java}, or else one with its name, since JBang does not need those
   * directories. Of several, the one nearest to the imported directory is preferred.
   *
   * @param projectDir the directory of the project
   * @param importDir the imported directory, e.g. the one with the jar of the function
   * @param className the name of the class, e.g. {@code com.myudf.MyScalarFunction}
   */
  static Optional<Path> find(Path projectDir, Path importDir, String className) {
    var topLevel = className.split("\\$")[0];
    var packagePath = Path.of(topLevel.replace('.', '/') + ".java");
    var fileName = packagePath.getFileName();
    var withPackage = new ArrayList<Path>();
    var withName = new ArrayList<Path>();
    try {
      Files.walkFileTree(
          projectDir,
          Set.of(),
          MAX_DEPTH,
          new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attributes) {
              var name = dir.getFileName().toString();
              return !dir.equals(projectDir) && (name.startsWith(".") || SKIPPED.contains(name))
                  ? FileVisitResult.SKIP_SUBTREE
                  : FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) {
              if (file.endsWith(packagePath)) {
                withPackage.add(file);
              } else if (file.getFileName().equals(fileName)) {
                withName.add(file);
              }
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (IOException e) {
      log.debug("Could not search {} for the source of {}", projectDir, className, e);
    }
    return (withPackage.isEmpty() ? withName : withPackage)
        .stream()
            .max(
                Comparator.comparingInt((Path file) -> sharedDirectories(importDir, file))
                    .thenComparing(Path::toString, Comparator.reverseOrder()));
  }

  /** The position of the name of a class in its source, e.g. after {@code public class}. */
  static Position declaration(String source, String className) {
    var simpleName = className.substring(className.lastIndexOf('.') + 1).split("\\$")[0];
    var declaration =
        Pattern.compile(
                "\\b(?:class|interface|record|enum)\\s+(" + Pattern.quote(simpleName) + ")\\b")
            .matcher(source);
    if (!declaration.find()) {
      return new Position(0, 0);
    }
    var before = source.substring(0, declaration.start(1));
    var line = (int) before.chars().filter(c -> c == '\n').count();
    return new Position(line, declaration.start(1) - (before.lastIndexOf('\n') + 1));
  }

  private static int sharedDirectories(Path dir, Path file) {
    var shared = 0;
    while (shared < dir.getNameCount()
        && shared < file.getNameCount() - 1
        && dir.getName(shared).equals(file.getName(shared))) {
      shared++;
    }
    return shared;
  }
}
