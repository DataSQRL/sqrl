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
package com.datasqrl.cli;

import com.datasqrl.lsp.ScriptFormatter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

@Command(
    name = "format",
    description =
        "Formats SQRL scripts: indents their lines by the structure of their queries and"
            + " normalizes the spaces between tokens.")
public class FormatCmd implements Callable<Integer> {

  private static final String INDENT = "    ";

  /** Directories of build output and tools, besides hidden ones */
  private static final Set<String> SKIPPED_DIRS = Set.of("build", "output", "node_modules");

  @ParentCommand DatasqrlCli cli;

  @Parameters(
      description = "The scripts, or directories to search for scripts (default: the current one).")
  List<Path> paths = new ArrayList<>();

  @Option(
      names = "--check",
      description =
          "Lists the scripts that are not formatted and fails, instead of formatting them.")
  boolean check;

  PrintStream out = System.out;

  @Override
  public Integer call() throws IOException {
    var root = cli.getWorkspaceDir();
    var unformatted = 0;
    var failed = 0;
    for (var script : scripts(root)) {
      var name = root.relativize(script);
      var text = Files.readString(script);
      var formatted = ScriptFormatter.format(text, INDENT);
      if (formatted.isEmpty()) {
        out.println(name + ": could not be formatted, e.g. a string is not terminated");
        failed++;
      } else if (!formatted.get().equals(text)) {
        unformatted++;
        if (check) {
          out.println(name + ":" + firstDifferentLine(text, formatted.get()) + ": not formatted");
        } else {
          Files.writeString(script, formatted.get());
          out.println("Formatted " + name);
        }
      }
    }
    if (check && unformatted > 0) {
      out.println(unformatted + " script(s) not formatted, run `sqrl format` to format them");
    }
    return failed > 0 || (check && unformatted > 0) ? 1 : 0;
  }

  private List<Path> scripts(Path root) throws IOException {
    var scripts = new ArrayList<Path>();
    for (var path : paths.isEmpty() ? List.of(Path.of("")) : paths) {
      var resolved = root.resolve(path).normalize();
      if (Files.isDirectory(resolved)) {
        Files.walkFileTree(
            resolved,
            new SimpleFileVisitor<>() {
              @Override
              public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attributes) {
                var name = dir.getFileName() == null ? "" : dir.getFileName().toString();
                return dir.equals(resolved)
                        || !(name.startsWith(".") || SKIPPED_DIRS.contains(name))
                    ? FileVisitResult.CONTINUE
                    : FileVisitResult.SKIP_SUBTREE;
              }

              @Override
              public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) {
                if (file.getFileName().toString().endsWith(".sqrl")) {
                  scripts.add(file);
                }
                return FileVisitResult.CONTINUE;
              }
            });
      } else if (Files.isRegularFile(resolved)) {
        scripts.add(resolved);
      } else {
        throw new IOException(path + " does not exist");
      }
    }
    return scripts.stream().sorted().toList();
  }

  private static int firstDifferentLine(String text, String formatted) {
    var lines = text.split("\n", -1);
    var formattedLines = formatted.split("\n", -1);
    for (int i = 0; i < Math.min(lines.length, formattedLines.length); i++) {
      if (!lines[i].equals(formattedLines[i])) {
        return i + 1;
      }
    }
    return Math.min(lines.length, formattedLines.length);
  }
}
