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
package com.datasqrl;

import static org.assertj.core.api.Assertions.fail;

import com.datasqrl.cli.AssertStatusHook;
import com.datasqrl.cli.DatasqrlCli;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class SqrlScriptExecutor {

  private Path rootDir;
  private String goal;
  private String script;
  private String graphql;
  private String testSuffix;
  private String testPath;
  private String packageJsonPath;
  private List<String> additionalArgs;

  public void execute(AssertStatusHook hook) {
    List<String> argsList = new ArrayList<>();
    argsList.add(goal);
    argsList.add(script);
    if (graphql != null && !graphql.isEmpty()) {
      argsList.add(graphql);
    }
    if (additionalArgs != null && !additionalArgs.isEmpty()) {
      argsList.addAll(additionalArgs);
    }
    if (getPackageJsonPath() != null) {
      argsList.addAll(Arrays.asList("-c", getPackageJsonPath()));
    }
    // Execute the command
    var cli = new DatasqrlCli(rootDir, hook, true);
    var exitCode =
        cli.getCmd().execute(argsList.toArray(new String[0])) + (hook.isSuccess() ? 0 : 1);
    if (exitCode != 0) {
      if (hook.failure() != null) {
        fail("", hook.failure());
      }
      fail("");
    }
  }
}
