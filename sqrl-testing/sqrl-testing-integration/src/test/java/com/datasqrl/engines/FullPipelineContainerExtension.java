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
package com.datasqrl.engines;

import com.datasqrl.engines.TestContainersForTestGoal.TestContainerHook;
import com.datasqrl.engines.TestEngine.EngineFactory;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

/**
 * JUnit 5 extension that manages {@link TestContainerHook} lifecycle for the duration of a test
 * class run. Containers are started once before all tests and torn down after all tests complete.
 * Between tests, state is cleared via {@link TestContainerHook#clear()}.
 *
 * <p>Inject {@link TestContainerHook} into test methods to access the active hook.
 */
public class FullPipelineContainerExtension
    implements BeforeAllCallback, AfterAllCallback, AfterEachCallback, ParameterResolver {

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(FullPipelineContainerExtension.class);

  @Override
  public void beforeAll(ExtensionContext context) {
    var engines = new EngineFactory().createAll();
    var hook = engines.accept(new TestContainersForTestGoal(), null);
    hook.start();
    getStore(context).put(getHookKey(context), hook);
  }

  @Override
  public void afterAll(ExtensionContext context) {
    var hook = getStore(context).remove(getHookKey(context), TestContainerHook.class);
    if (hook != null) {
      hook.teardown();
    }
  }

  @Override
  public void afterEach(ExtensionContext context) {
    var hook = getHook(context);
    if (hook != null) {
      hook.clear();
    }
  }

  @Override
  public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext context) {
    return parameterContext.getParameter().getType().equals(TestContainerHook.class);
  }

  @Override
  public Object resolveParameter(ParameterContext parameterContext, ExtensionContext context) {
    var hook = getHook(context);
    if (hook == null) {
      throw new ParameterResolutionException(
          "No TestContainerHook is available for " + context.getRequiredTestClass().getName());
    }
    return hook;
  }

  private ExtensionContext.Store getStore(ExtensionContext context) {
    return context.getRoot().getStore(NAMESPACE);
  }

  private TestContainerHook getHook(ExtensionContext context) {
    return getStore(context).get(getHookKey(context), TestContainerHook.class);
  }

  private Class<?> getHookKey(ExtensionContext context) {
    return context.getRequiredTestClass();
  }
}
