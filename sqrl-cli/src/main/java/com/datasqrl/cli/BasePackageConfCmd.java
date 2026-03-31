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
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public abstract class BasePackageConfCmd extends BaseOsProcessManagerCmd {

  @Parameters(
      arity = "0..*",
      description = "Package configuration file(s) of the project. Default: \"package.json\".",
      scope = CommandLine.ScopeType.INHERIT)
  protected List<Path> packageFiles = Collections.emptyList();

  @Option(
      names = {"-B", "--batch-output"},
      description = "Run in batch output mode (disables colored output).")
  protected boolean batchMode = false;

  protected OutputFormatter getOutputFormatter() {
    return cli.internalTestExec
        ? new NoOutputFormatter()
        : new DefaultOutputFormatter(cli.rootDir, batchMode);
  }
}
