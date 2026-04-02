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

import com.datasqrl.config.SqrlConstants;
import com.datasqrl.util.OsProcessManager;
import java.nio.file.Path;
import picocli.CommandLine.Option;

public abstract class BaseOsProcessManagerCmd extends BaseCmd {

  protected static final Path DEFAULT_TARGET =
      Path.of(SqrlConstants.BUILD_DIR_NAME, SqrlConstants.DEPLOY_DIR_NAME);

  @Option(
      names = {"-t", "--target"},
      description = "Target folder for deployment artifacts and plans. Default: \"build/deploy\".")
  protected Path targetFolder = DEFAULT_TARGET;

  @Override
  protected void teardown() {
    if (!cli.internalTestExec) {
      getOsProcessManager().teardown(getBuildDir());
    }
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
