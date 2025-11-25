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

import com.datasqrl.cli.output.OutputFormatter;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.util.OsProcessManager;
import com.google.inject.ProvisionException;
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

  @CommandLine.Option(
      names = {"-B", "--batch-mode"},
      description = "Run in batch mode (disable colored output)")
  protected boolean batchMode = false;

  protected final AtomicInteger exitCode = new AtomicInteger(0);
  protected long startTime;

  @Override
  @SneakyThrows
  public void run() {
    startTime = System.currentTimeMillis();
    ErrorCollector collector = ErrorCollector.root();
    try {
      runInternal(collector);
      cli.statusHook.onSuccess(collector);

    } catch (ProvisionException | CollectedException e) {
      var ce = unwrapCollectedException(e);
      if (ce.isInternalError()) {
        ce.printStackTrace();
      }
      cli.statusHook.onFailure(ce, collector);

    } catch (Throwable e) { // unknown exception
      collector.getCatcher().handle(e);
      e.printStackTrace();
      cli.statusHook.onFailure(e, collector);
    }

    if (collector.hasErrors()) {
      exitCode.set(1);
    }

    if (!cli.internalTestExec) {
      getOsProcessManager().teardown(getBuildDir());
    }

    System.out.println(ErrorPrinter.prettyPrint(collector));
  }

  protected abstract void runInternal(ErrorCollector errors) throws Exception;

  protected OutputFormatter getOutputFormatter() {
    return new OutputFormatter(batchMode);
  }

  protected long getElapsedTime() {
    return System.currentTimeMillis() - startTime;
  }

  protected OsProcessManager getOsProcessManager() {
    return new OsProcessManager(System.getenv());
  }

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

  /**
   * Unwraps {@link CollectedException} from Guice {@link ProvisionException} wrappers to provide
   * clean error messages.
   *
   * <p>Validation errors during dependency injection (e.g., in pipeline configuration) are thrown
   * as {@link CollectedException} with clear messages. Guice wraps them in {@link
   * ProvisionException}, obscuring the original error with verbose DI stack traces.
   *
   * @param e the runtime exception that may be a CollectedException or contain one as a cause
   * @return the unwrapped CollectedException
   * @throws RuntimeException the original exception if it doesn't contain a CollectedException
   */
  private CollectedException unwrapCollectedException(RuntimeException e) {
    if (e instanceof CollectedException ce) {
      return ce;
    }

    if (e.getCause() instanceof CollectedException ce) {
      return ce;
    }

    throw e;
  }
}
