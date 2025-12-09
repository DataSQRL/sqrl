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

import com.datasqrl.cli.output.DefaultOutputFormatter;
import com.datasqrl.cli.output.NoOutputFormatter;
import com.datasqrl.cli.output.OutputFormatter;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.util.OsProcessManager;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public abstract class BasePackageConfCmd extends BaseCmd {

  protected static final Path DEFAULT_TARGET =
      Path.of(SqrlConstants.BUILD_DIR_NAME, SqrlConstants.DEPLOY_DIR_NAME);

  @Parameters(
      arity = "0..*",
      description = "Package configuration file(s) of the project. Default: \"package.json\".",
      scope = CommandLine.ScopeType.INHERIT)
  protected List<Path> packageFiles = Collections.emptyList();

  @Option(
      names = {"-t", "--target"},
      description = "Target folder for deployment artifacts and plans. Default: \"build/deploy\".")
  protected Path targetFolder = DEFAULT_TARGET;

  @Option(
      names = {"-B", "--batch-output"},
      description = "Run in batch output mode (disables colored output).")
  protected boolean batchMode = false;

  @Override
  protected void teardown() {
    if (!cli.internalTestExec) {
      getOsProcessManager().teardown(getBuildDir());
    }
  }

  protected OutputFormatter getOutputFormatter() {
    return cli.internalTestExec ? new NoOutputFormatter() : new DefaultOutputFormatter(batchMode);
  }

  protected OsProcessManager getOsProcessManager() {
    return new OsProcessManager(System.getenv());
  }

  protected Path getTargetFolder() {
    if (targetFolder.isAbsolute()) {
      return targetFolder;
    }

    return cli.rootDir.resolve(targetFolder);
  }
}
