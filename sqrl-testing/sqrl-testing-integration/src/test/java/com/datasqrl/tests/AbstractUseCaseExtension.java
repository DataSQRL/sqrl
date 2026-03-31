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
package com.datasqrl.tests;

import com.datasqrl.UseCaseParam;
import java.lang.reflect.Method;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;

/**
 * Base JUnit 5 interceptor for use-case-specific setup and teardown around test invocations.
 * Implementations opt into matching {@link UseCaseParam} values via {@link
 * #supports(UseCaseParam)}.
 */
abstract class AbstractUseCaseExtension implements InvocationInterceptor {

  @Override
  public void interceptTestMethod(
      Invocation<Void> invocation,
      ReflectiveInvocationContext<Method> invocationCtx,
      ExtensionContext extensionCtx)
      throws Throwable {
    intercept(invocation, invocationCtx, extensionCtx);
  }

  @Override
  public void interceptTestTemplateMethod(
      Invocation<Void> invocation,
      ReflectiveInvocationContext<Method> invocationCtx,
      ExtensionContext extensionCtx)
      throws Throwable {
    intercept(invocation, invocationCtx, extensionCtx);
  }

  private void intercept(
      Invocation<Void> invocation,
      ReflectiveInvocationContext<Method> invocationCtx,
      ExtensionContext extensionCtx)
      throws Throwable {
    var useCaseParam = getUseCaseParam(invocationCtx);
    if (useCaseParam == null || !supports(useCaseParam)) {
      invocation.proceed();
      return;
    }

    Throwable failure = null;
    try {
      setup(useCaseParam);
      invocation.proceed();
    } catch (Throwable throwable) {
      failure = throwable;
      throw throwable;
    } finally {
      try {
        teardown(useCaseParam);
      } catch (Throwable cleanupFailure) {
        if (failure != null) {
          failure.addSuppressed(cleanupFailure);
        } else {
          throw cleanupFailure;
        }
      }
    }
  }

  private UseCaseParam getUseCaseParam(ReflectiveInvocationContext<Method> invocationCtx) {
    var arguments = invocationCtx.getArguments();
    if (arguments.isEmpty()) {
      return null;
    }

    var argument = arguments.get(0);
    if (argument instanceof UseCaseParam useCaseParam) {
      return useCaseParam;
    }
    return null;
  }

  /** Returns whether this extension should wrap the current use case invocation. */
  protected abstract boolean supports(UseCaseParam param);

  /** Hook invoked before the wrapped use case test runs. */
  protected abstract void setup(UseCaseParam param);

  /** Hook invoked after the wrapped use case test finishes. */
  protected abstract void teardown(UseCaseParam param);
}
