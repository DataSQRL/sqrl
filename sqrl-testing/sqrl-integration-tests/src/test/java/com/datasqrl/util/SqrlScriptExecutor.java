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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datasqrl.cli.AssertStatusHook;
import com.datasqrl.cli.DatasqrlCli;
import java.nio.file.Path;

public record SqrlScriptExecutor(Path packageJsonPath, String goal) {

  public void execute(AssertStatusHook hook) {
    assertThat(packageJsonPath).as("Package JSON must be provided").exists();
    var rootDir = packageJsonPath.getParent();
    var fileName = packageJsonPath.getFileName().toString();

    // Execute the command
    var cli = new DatasqrlCli(rootDir, hook, true);
    var exitCode = cli.getCmd().execute(goal, "-c", fileName);
    exitCode += hook.isSuccess() ? 0 : 1;

    if (exitCode != 0) {
      if (hook.failure() != null) {
        fail("", hook.failure());
      }
      fail("");
    }
  }
}
