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

import com.datasqrl.config.SqrlConstants;
import com.datasqrl.loaders.ModuleLoaderImpl;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.lsp4j.SymbolKind;

/**
 * What IMPORT statements can import from the build directory of a check, which holds the scripts
 * and table files of the project and of its packages: table files, the tables of scripts, and
 * scripts, which are imported as a database. Only the {@code CREATE TABLE} tables of a script can
 * be imported by name, its other tables by importing all of it.
 */
@Slf4j
@RequiredArgsConstructor
class ImportCandidates {

  private static final int MAX_DEPTH = 6;
  private static final String SQRL_EXTENSION = ModuleLoaderImpl.SQRL_FILE_EXTENSION;

  /** The outline kinds of tables and functions that a script defines with a query */
  private static final Set<SymbolKind> DEFINITION_KINDS =
      Set.of(SymbolKind.Class, SymbolKind.Function);

  private final Path buildDir;

  /** The name of the script that imports, which cannot import itself */
  private final Path script;

  /**
   * The import paths of the tables with a name, ignoring case, e.g. {@code shop.sources.Orders}, or
   * {@code shop.sources.*} for a table that the script defines with a query.
   */
  List<String> tables(String name) {
    var result = new LinkedHashSet<String>();
    for (var file : files()) {
      var fileName = file.getFileName().toString();
      if (fileName.endsWith(ModuleLoaderImpl.TABLE_FILE_SUFFIX)) {
        var table = strip(fileName, ModuleLoaderImpl.TABLE_FILE_SUFFIX);
        if (table.equalsIgnoreCase(name)) {
          result.add(importPath(file.getParent(), table));
        }
      } else if (fileName.endsWith(ModuleLoaderImpl.SQRL_FILE_EXTENSION) && !isScript(file)) {
        String text;
        try {
          text = Files.readString(file);
        } catch (IOException e) {
          log.debug("Could not read {}", file, e);
          continue;
        }
        var module = importPath(file.getParent(), strip(fileName, SQRL_EXTENSION));
        for (var symbol : DocumentOutline.of(text, file, List.of())) {
          if (symbol.getName().equalsIgnoreCase(name)) {
            if (symbol.getKind() == SymbolKind.Struct) {
              result.add(module + "." + symbol.getName());
            } else if (DEFINITION_KINDS.contains(symbol.getKind())) {
              result.add(module + ".*");
            }
          }
        }
      }
    }
    return List.copyOf(result);
  }

  /** The import paths of the scripts with a name, ignoring case, e.g. {@code shop.sources}. */
  List<String> scripts(String name) {
    return files().stream()
        .filter(file -> !isScript(file))
        .filter(file -> file.getFileName().toString().equalsIgnoreCase(name + SQRL_EXTENSION))
        .map(
            file ->
                importPath(file.getParent(), strip(file.getFileName().toString(), SQRL_EXTENSION)))
        .toList();
  }

  /** The scripts and table files, except the deployment assets of earlier compiles. */
  private List<Path> files() {
    if (!Files.isDirectory(buildDir)) {
      return List.of();
    }
    var deployDir = buildDir.resolve(SqrlConstants.DEPLOY_DIR_NAME);
    try (var files = Files.walk(buildDir, MAX_DEPTH)) {
      return files
          .filter(file -> !file.startsWith(deployDir))
          .filter(file -> !isHidden(buildDir.relativize(file)))
          .filter(
              file -> {
                var name = file.getFileName().toString();
                return name.endsWith(SQRL_EXTENSION)
                    || name.endsWith(ModuleLoaderImpl.TABLE_FILE_SUFFIX);
              })
          .filter(Files::isRegularFile)
          .sorted()
          .toList();
    } catch (IOException | RuntimeException e) {
      log.debug("Could not list {}", buildDir, e);
      return List.of();
    }
  }

  /** Whether a file in the build directory is the copy of the script that imports. */
  private boolean isScript(Path file) {
    return file.getParent().equals(buildDir) && file.getFileName().equals(script.getFileName());
  }

  /** The path of a file or table in a directory of the build directory, separated by dots. */
  private String importPath(Path dir, String name) {
    var parts =
        StreamSupport.stream(buildDir.relativize(dir).spliterator(), false)
            .map(Path::toString)
            .filter(part -> !part.isEmpty())
            .collect(Collectors.toCollection(ArrayList::new));
    parts.add(name);
    return String.join(".", parts);
  }

  private static boolean isHidden(Path relative) {
    return StreamSupport.stream(relative.spliterator(), false)
        .anyMatch(part -> part.toString().startsWith("."));
  }

  private static String strip(String name, String suffix) {
    return name.substring(0, name.length() - suffix.length());
  }
}
