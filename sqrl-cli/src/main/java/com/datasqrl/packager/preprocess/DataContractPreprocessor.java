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

import com.datasqrl.config.SqrlConstants;
import com.datasqrl.config.WorkspacePaths;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.FilePreprocessingPipeline;
import com.datasqrl.planner.contract.DataContractLoader;
import com.datasqrl.planner.contract.OdcsConstants;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.Strings;
import org.springframework.stereotype.Component;

/**
 * Copies the data contract files of a project into the build folder, where the planner reads the
 * contract next to a script, and reports the files that the planner would never read as a contract:
 * a file with an extension that is not read, and a contract file without a script of its name next
 * to it. Without the error, such a contract would be ignored silently and look like a contract that
 * passed.
 */
@Component
@RequiredArgsConstructor
public class DataContractPreprocessor implements Preprocessor {

  private final WorkspacePaths workspacePaths;
  private final ErrorCollector errors;

  // State of the current pipeline run, reset by complete()
  private final List<Path> contractFiles = new ArrayList<>();

  /** The names, without extension and in lower case, of the files that define tables, by folder */
  private final Map<Path, Set<String>> scriptNames = new HashMap<>();

  private boolean hasReported;

  @Override
  public void process(Path file, FilePreprocessingPipeline.Context ctx) {
    var fileName = file.getFileName().toString();
    DataContractLoader.stemOf(fileName)
        .ifPresent(
            name ->
                scriptNames
                    .computeIfAbsent(file.getParent(), folder -> new HashSet<>())
                    .add(name.toLowerCase(Locale.ROOT)));

    if (fileName.endsWith(OdcsConstants.FILE_SUFFIX)) {
      if (!isCompilerOutput(file)) {
        ctx.copyToBuild(file);
        contractFiles.add(file);
      }
      return;
    }
    OdcsConstants.UNSUPPORTED_FILE_EXTENSIONS.stream()
        .map(extension -> "." + extension)
        .filter(suffix -> fileName.toLowerCase(Locale.ROOT).endsWith(suffix))
        .findFirst()
        .ifPresent(
            suffix ->
                report(
                    "File [%s] looks like a data contract but is not read. Rename it to [%s]",
                    shownPath(file),
                    fileName.substring(0, fileName.length() - suffix.length())
                        + OdcsConstants.FILE_SUFFIX));
  }

  /** All contract files that would not be read are reported, then the compilation fails. */
  @Override
  public void complete() {
    try {
      contractFiles.stream().filter(file -> !hasScript(file)).forEach(this::reportNoScript);
      if (hasReported) {
        errors.abortOnErrors();
      }
    } finally {
      // The same instance serves every pipeline run: included projects first, then the project
      contractFiles.clear();
      scriptNames.clear();
      hasReported = false;
    }
  }

  /**
   * The contracts that an earlier compilation wrote are output and not contracts of the project.
   * They are in the {@code contracts} folder next to the {@code plan} folder of a target folder,
   * which can be a folder of the project, or the build folder of an included project.
   */
  private static boolean isCompilerOutput(Path file) {
    var folder = file.toAbsolutePath().getParent();
    return folder.getFileName().toString().equals(SqrlConstants.CONTRACTS_DIR)
        && Files.isDirectory(folder.resolveSibling(SqrlConstants.PLAN_DIR));
  }

  /** Whether a file that defines tables with the name of the contract file is in its folder. */
  private boolean hasScript(Path contractFile) {
    var name =
        Strings.CS.removeEnd(contractFile.getFileName().toString(), OdcsConstants.FILE_SUFFIX);
    return scriptNames
        .getOrDefault(contractFile.getParent(), Set.of())
        .contains(name.toLowerCase(Locale.ROOT));
  }

  private void reportNoScript(Path contractFile) {
    var name =
        Strings.CS.removeEnd(contractFile.getFileName().toString(), OdcsConstants.FILE_SUFFIX);
    report(
        "Contract file [%s] has no SQRL file next to it. A contract applies to the tables of the"
            + " script with the same name: expected %s.sqrl or %s.table.sql in the same folder",
        shownPath(contractFile), name, name);
  }

  private void report(String message, Object... args) {
    hasReported = true;
    errors.abortOnFatal(false).fatal(ErrorCode.INVALID_DATA_CONTRACT, message, args);
  }

  /** The path from the project root, or the path as it is for a file of an included project. */
  private Path shownPath(Path file) {
    var projectRoot = workspacePaths.projectRoot().toAbsolutePath().normalize();
    var path = file.toAbsolutePath().normalize();
    return path.startsWith(projectRoot) ? projectRoot.relativize(path) : path;
  }
}
