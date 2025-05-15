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
package com.datasqrl.cmd;

import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import picocli.CommandLine.IExitCodeGenerator;

@Slf4j
public abstract class AbstractCommand implements Runnable, IExitCodeGenerator {

  @CommandLine.ParentCommand protected RootCommand root;
  public AtomicInteger exitCode = new AtomicInteger(0);

  @Override
  @SneakyThrows
  public void run() {
    ErrorCollector collector = ErrorCollector.root();
    try {
      execute(collector);
      root.statusHook.onSuccess(collector);
    } catch (CollectedException e) {
      if (e.isInternalError()) e.printStackTrace();
      e.printStackTrace();
      root.statusHook.onFailure(e, collector);
    } catch (Throwable e) { // unknown exception
      collector.getCatcher().handle(e);
      e.printStackTrace();
      root.statusHook.onFailure(e, collector);
    }
    if (collector.hasErrors()) exitCode.set(1);
    System.out.println(ErrorPrinter.prettyPrint(collector));
  }

  protected abstract void execute(ErrorCollector errors) throws Exception;

  @Override
  public int getExitCode() {
    return exitCode.get();
  }
}
