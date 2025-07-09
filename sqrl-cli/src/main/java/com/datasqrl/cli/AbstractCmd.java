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
import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import picocli.CommandLine;
import picocli.CommandLine.IExitCodeGenerator;

public abstract class AbstractCmd implements Runnable, IExitCodeGenerator {

  protected static final Path DEFAULT_TARGET_DIR =
      Path.of(SqrlConstants.BUILD_DIR_NAME, SqrlConstants.DEPLOY_DIR_NAME);

  @CommandLine.ParentCommand protected DatasqrlCli cli;

  @CommandLine.Option(
      names = {"-t", "--target"},
      description = "Target directory for deployment artifacts and plans")
  protected Path targetDir = DEFAULT_TARGET_DIR;

  private final AtomicInteger exitCode = new AtomicInteger(0);

  @Override
  @SneakyThrows
  public void run() {
    ErrorCollector collector = ErrorCollector.root();
    try {
      runInternal(collector);
      cli.statusHook.onSuccess(collector);
    } catch (CollectedException e) {
      if (e.isInternalError()) e.printStackTrace();
      e.printStackTrace();
      cli.statusHook.onFailure(e, collector);
    } catch (Throwable e) { // unknown exception
      collector.getCatcher().handle(e);
      e.printStackTrace();
      cli.statusHook.onFailure(e, collector);
    }
    if (collector.hasErrors()) exitCode.set(1);
    System.out.println(ErrorPrinter.prettyPrint(collector));
  }

  protected abstract void runInternal(ErrorCollector errors) throws Exception;

  protected Path getBuildDir() {
    return cli.rootDir.resolve(SqrlConstants.BUILD_DIR_NAME);
  }

  protected Path getTargetDir() {
    if (targetDir.isAbsolute()) {
      return targetDir;
    }

    return cli.rootDir.resolve(targetDir);
  }

  @Override
  public int getExitCode() {
    return exitCode.get();
  }
}
