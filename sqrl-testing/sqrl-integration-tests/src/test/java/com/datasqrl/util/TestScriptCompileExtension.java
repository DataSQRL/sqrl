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

import com.datasqrl.UseCaseParam;
import com.datasqrl.cli.AssertStatusHook;
import java.lang.reflect.Method;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;

/**
 * JUnit 5 extension that handles SQRL script compilation execution.
 *
 * <p>This extension automatically executes SQRL script compilation for parameterized tests that use
 * UseCaseParam as the first parameter. It runs after the TestShardingExtension has decided to
 * proceed with the test, but before the actual test method executes.
 *
 * <p>Usage:
 *
 * <pre>
 * &#64;ExtendWith({TestShardingExtension.class, TestScriptCompileExtension.class})
 * class MyTest {
 *   &#64;ParameterizedTest
 *   void testMethod(UseCaseParam param) {
 *     // Script compilation happens automatically before this method runs
 *   }
 * }
 * </pre>
 */
@Slf4j
public class TestScriptCompileExtension implements InvocationInterceptor {

  @Override
  public void interceptTestTemplateMethod(
      Invocation<Void> invocation,
      ReflectiveInvocationContext<Method> invocationCtx,
      ExtensionContext extensionCtx)
      throws Throwable {

    // Get the test parameters to extract UseCaseParam
    var arguments = invocationCtx.getArguments();
    if (arguments.isEmpty() || !(arguments.get(0) instanceof UseCaseParam param)) {
      // No UseCaseParam found, just proceed without script compilation
      invocation.proceed();
      return;
    }

    log.debug("Executing script compilation for {}", param.getPackageJsonName());

    // Execute the script compilation
    executeScript(param);

    // Proceed to the actual test method
    invocation.proceed();
  }

  private void executeScript(UseCaseParam param) {
    SqrlScriptExecutor executor = new SqrlScriptExecutor(param.packageJsonPath(), param.goal());
    AssertStatusHook hook = new AssertStatusHook();
    try {
      executor.execute(hook);
    } catch (Throwable e) {
      if (hook.failure() != null) {
        e.addSuppressed(hook.failure());
      }
      throw e;
    }
  }
}
