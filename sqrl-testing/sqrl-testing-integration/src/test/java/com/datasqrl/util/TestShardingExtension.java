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

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.datasqrl.UseCaseParam;
import java.lang.reflect.Method;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;

/**
 * JUnit5 extension that implements test sharding based on environment variables.
 *
 * <p>This extension reads the following environment variables:
 *
 * <ul>
 *   <li>{@code TEST_SHARDING_TOTAL} - Total number of shards
 *   <li>{@code TEST_SHARDING_INDEX} - Current shard index (0-based)
 * </ul>
 *
 * <p>When sharding is enabled, tests are distributed across shards using a hash-based approach. For
 * parameterized tests, the test parameters are used for hashing. For regular tests, the test method
 * name is used. Tests not assigned to the current shard are skipped using JUnit assumptions.
 */
@Slf4j
public class TestShardingExtension implements InvocationInterceptor {

  private static final String ENV_SHARDING_TOTAL = "TEST_SHARDING_TOTAL";
  private static final String ENV_SHARDING_INDEX = "TEST_SHARDING_INDEX";

  private final Integer totalShards;
  private final Integer shardIndex;

  public TestShardingExtension() {
    var total = System.getenv(ENV_SHARDING_TOTAL);
    var index = System.getenv(ENV_SHARDING_INDEX);

    if (ObjectUtils.isEmpty(total) || ObjectUtils.isEmpty(index)) {
      log.debug("Test sharding disabled - environment variables not set");
      this.totalShards = null;
      this.shardIndex = null;
      return;
    }

    this.totalShards = Integer.parseInt(total);
    this.shardIndex = Integer.parseInt(index);

    log.info("Test sharding enabled: total={}, index={}", totalShards, shardIndex);
  }

  @Override
  public void interceptTestMethod(
      Invocation<Void> invocation,
      ReflectiveInvocationContext<Method> invocationCtx,
      ExtensionContext extensionCtx)
      throws Throwable {

    handleSharding(invocation, invocationCtx, extensionCtx);
  }

  @Override
  public void interceptTestTemplateMethod(
      Invocation<Void> invocation,
      ReflectiveInvocationContext<Method> invocationCtx,
      ExtensionContext extensionCtx)
      throws Throwable {

    handleSharding(invocation, invocationCtx, extensionCtx);
  }

  private void handleSharding(
      Invocation<Void> invocation,
      ReflectiveInvocationContext<Method> invocationCtx,
      ExtensionContext extensionCtx)
      throws Throwable {

    if (totalShards == null) {
      invocation.proceed();
      return;
    }

    int shardAssignment = calculateShardAssignment(invocationCtx, extensionCtx);

    if (shardAssignment == shardIndex) {
      invocation.proceed();
    } else {
      //noinspection DataFlowIssue
      assumeTrue(
          false,
          String.format(
              "Skipping due to test sharding. Test assigned to shard %d, current shard %d",
              shardAssignment, shardIndex));
    }
  }

  private int calculateShardAssignment(
      ReflectiveInvocationContext<Method> invocationCtx, ExtensionContext extensionCtx) {

    var arguments = invocationCtx.getArguments();

    if (arguments.isEmpty()) {
      // Regular test: use method name + class name for shard calculation
      var testIdentifier =
          extensionCtx.getRequiredTestClass().getName()
              + "#"
              + extensionCtx.getRequiredTestMethod().getName();
      return Math.abs(testIdentifier.hashCode()) % totalShards;
    }

    // Parameterized test: check if first parameter is UseCaseParam with index
    var testParam = arguments.get(0);
    if (testParam instanceof UseCaseParam useCaseParam) {
      // Use index for even distribution across shards
      return useCaseParam.index() % totalShards;
    }

    // Fallback to hash-based approach for other parameter types
    return Math.abs(testParam.hashCode()) % totalShards;
  }
}
