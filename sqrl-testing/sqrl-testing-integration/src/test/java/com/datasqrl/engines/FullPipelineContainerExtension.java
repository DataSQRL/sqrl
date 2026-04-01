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

/**
 * JUnit 5 extension that manages {@link TestContainerHook} lifecycle for the duration of a test
 * class run. Containers are started once before all tests and torn down after all tests complete.
 * Between tests, state is cleared via {@link TestContainerHook#clear()}.
 *
 * <p>Use {@link #getHook()} to retrieve the active hook from within a test method.
 */
public class FullPipelineContainerExtension
    implements BeforeAllCallback, AfterAllCallback, AfterEachCallback {

  private static TestContainerHook hook;

  @Override
  public void beforeAll(ExtensionContext context) {
    var engines = new EngineFactory().createAll();
    hook = engines.accept(new TestContainersForTestGoal(), null);
    hook.start();
  }

  @Override
  public void afterAll(ExtensionContext context) {
    if (hook != null) {
      hook.teardown();
    }
  }

  @Override
  public void afterEach(ExtensionContext context) {
    if (hook != null) {
      hook.clear();
    }
  }

  public static TestContainerHook getHook() {
    return hook;
  }
}
