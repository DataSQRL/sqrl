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
import org.springframework.beans.factory.BeanCreationException;
import picocli.CommandLine.IExitCodeGenerator;
import picocli.CommandLine.ParentCommand;

public abstract class BaseCmd implements Runnable, IExitCodeGenerator {

  @ParentCommand protected DatasqrlCli cli;

  protected final AtomicInteger exitCode = new AtomicInteger(0);

  private long startTime;

  @Override
  @SneakyThrows
  public void run() {
    startTime = System.currentTimeMillis();
    var collector = ErrorCollector.root();

    try {
      runInternal(collector);
      cli.statusHook.onSuccess(collector);

    } catch (BeanCreationException | CollectedException e) {
      var ce = unwrapCollectedException(e);
      if (ce.isInternalError()) {
        ce.printStackTrace();
      }
      cli.statusHook.onFailure(ce, collector);

    } catch (Throwable e) { // unknown exception
      collector.getCatcher().handle(e);
      e.printStackTrace();
      cli.statusHook.onFailure(e, collector);

    } finally {
      teardown();
    }

    if (collector.hasErrors()) {
      exitCode.set(1);
    }

    System.out.println(ErrorPrinter.prettyPrint(collector));
  }

  protected abstract void runInternal(ErrorCollector errors) throws Exception;

  protected void teardown() {
    // By default, do nothing
  }

  protected long getElapsedTime() {
    return System.currentTimeMillis() - startTime;
  }

  protected Path getBuildDir() {
    return cli.rootDir.resolve(SqrlConstants.BUILD_DIR_NAME);
  }

  @Override
  public int getExitCode() {
    return exitCode.get();
  }

  /**
   * Unwraps {@link CollectedException} from Spring {@link BeanCreationException} wrappers to
   * provide clean error messages.
   *
   * <p>Validation errors during dependency injection (e.g., in pipeline configuration) are thrown
   * as {@link CollectedException} with clear messages. Spring wraps them in multiple layers of
   * {@link BeanCreationException}, obscuring the original error with verbose DI stack traces.
   *
   * @param e the runtime exception that may be a CollectedException or contain one as a cause
   * @return the unwrapped CollectedException
   * @throws RuntimeException the original exception if it doesn't contain a CollectedException
   */
  private CollectedException unwrapCollectedException(RuntimeException e) {
    Throwable current = e;
    while (current != null) {
      if (current instanceof CollectedException ce) {
        return ce;
      }
      current = current.getCause();
    }
    throw e;
  }
}
