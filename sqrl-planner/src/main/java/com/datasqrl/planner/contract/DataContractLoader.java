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
package com.datasqrl.planner.contract;

import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.ModuleLoaderImpl;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import lombok.Getter;
import org.apache.commons.lang3.Strings;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * Loads the data contract files that sit next to the files that define tables: {@code
 * mytables.sqrl} pairs with {@code mytables.odcs.yaml}, for the extension of each {@link
 * DataContractFormat}. Every file is loaded once per compilation.
 *
 * <p>All errors of a contract file are recorded without aborting, so that the errors of all files
 * are reported together. A file with errors is not applied to any table.
 */
@Component
@Lazy
public class DataContractLoader {

  private final ErrorCollector errors;

  @Getter private final List<DataContractFormat> formats = DataContractFormat.all();

  /** The contract file of each file that defines tables, by the normalized path of that file */
  private final Map<Path, Optional<DataContractFile>> byDefiningFile = new HashMap<>();

  /** The contract files that were found, by path; empty for a file with errors */
  private final Map<Path, Optional<DataContractFile>> contractFiles = new LinkedHashMap<>();

  public DataContractLoader(ErrorCollector errors) {
    this.errors = errors.abortOnFatal(false);
  }

  /**
   * @param definingFile a file that defines tables, a script or a {@code .table.sql} file
   * @return the contract file next to it, empty if there is none or it has errors
   */
  public Optional<DataContractFile> forScript(Path definingFile) {
    var file = definingFile.toAbsolutePath().normalize();
    var contractFile = byDefiningFile.get(file);
    if (contractFile == null) {
      contractFile = find(file);
      byDefiningFile.put(file, contractFile);
    }
    return contractFile;
  }

  /** The valid contract files that were loaded, in the order they were first used. */
  public List<DataContractFile> getLoadedFiles() {
    return contractFiles.values().stream().flatMap(Optional::stream).toList();
  }

  /** The paths of all contract files that were found next to a planned file, valid or not. */
  public Set<Path> getFoundFiles() {
    return contractFiles.keySet();
  }

  /**
   * @return the name of a defining file without its extension, which is the name of its contract
   *     file without the extension of the format; empty if the file cannot define tables
   */
  public static Optional<String> stemOf(String fileName) {
    return Stream.of(ModuleLoaderImpl.TABLE_FILE_SUFFIX, ModuleLoaderImpl.SQRL_FILE_EXTENSION)
        .filter(fileName::endsWith)
        .map(suffix -> Strings.CS.removeEnd(fileName, suffix))
        .findFirst();
  }

  private Optional<DataContractFile> find(Path definingFile) {
    var stem = stemOf(definingFile.getFileName().toString());
    if (stem.isEmpty()) {
      return Optional.empty();
    }
    var names = fileNames(definingFile.getParent());
    var found =
        formats.stream()
            .flatMap(
                format ->
                    findContractFile(definingFile, format.fileName(stem.get()), names).stream()
                        .map(path -> Map.entry(path, format)))
            .toList();
    if (found.size() > 1) {
      // A table has one contract
      errors.fatal(
          ErrorCode.INVALID_DATA_CONTRACT,
          "[%s] has more than one contract file: %s. Keep one of them",
          definingFile.getFileName(),
          found.stream().map(entry -> entry.getKey().getFileName()).toList());
      return Optional.empty();
    }
    return found.stream().findFirst().flatMap(entry -> loadOnce(entry.getKey(), entry.getValue()));
  }

  private List<String> fileNames(Path folder) {
    try (var files = Files.list(folder)) {
      return files.map(path -> path.getFileName().toString()).toList();
    } catch (IOException e) {
      errors.fatal("Could not list the files of [%s]: %s", folder, e);
      return List.of();
    }
  }

  /**
   * Finds the contract file by comparing the names in the folder, so that the result does not
   * depend on whether the file system is case-sensitive.
   */
  private Optional<Path> findContractFile(Path definingFile, String expected, List<String> names) {
    if (names.contains(expected)) {
      return Optional.of(definingFile.resolveSibling(expected));
    }

    names.stream()
        .filter(name -> name.equalsIgnoreCase(expected))
        .forEach(
            name -> {
              // Such a pair works on a case-insensitive file system and not on others
              errors.fatal(
                  ErrorCode.INVALID_DATA_CONTRACT,
                  "Contract file [%s] differs from the name of [%s] only in case and is not"
                      + " read. Rename it to [%s]",
                  name,
                  definingFile.getFileName(),
                  expected);
              // The file is reported, so it is not also one that no script uses
              contractFiles.put(definingFile.resolveSibling(name), Optional.empty());
            });

    return Optional.empty();
  }

  /** Two files that define tables can share a contract file: a script and a table file. */
  private Optional<DataContractFile> loadOnce(Path contractFile, DataContractFormat format) {
    var loaded = contractFiles.get(contractFile);
    if (loaded == null) {
      loaded = load(contractFile, format);
      contractFiles.put(contractFile, loaded);
    }
    return loaded;
  }

  /** Reports every error of the file, and then once that the file is not applied. */
  private Optional<DataContractFile> load(Path path, DataContractFormat format) {
    String content;
    try {
      content = Files.readString(path);
    } catch (IOException e) {
      errors.fatal("Could not read contract file [%s]: %s", path, e);
      return Optional.empty();
    }

    var file = format.read(path, content, errors.withScript(path, content));
    if (file.isEmpty()) {
      errors.fatal(
          ErrorCode.INVALID_DATA_CONTRACT,
          "Contract file [%s] is not a valid data contract and is not applied",
          path.getFileName());
    }
    return file;
  }
}
