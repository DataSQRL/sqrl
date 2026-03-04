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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * JUnit5 extension that distributes tests across shards based on environment variables.
 *
 * <ul>
 *   <li>{@code TEST_SHARDING_TOTAL} - Total number of shards
 *   <li>{@code TEST_SHARDING_INDEX} - Current shard index (0-based)
 * </ul>
 *
 * <p>When sharding is enabled, tests are assigned to shards using a hash of their unique ID. For
 * parameterized tests, each invocation (parameter combination) is sharded independently, preventing
 * a single shard from running all invocations of a heavy parameterized test.
 */
@Slf4j
public class TestShardingExtension implements ExecutionCondition {

  private static final String ENV_SHARDING_TOTAL = "TEST_SHARDING_TOTAL";
  private static final String ENV_SHARDING_INDEX = "TEST_SHARDING_INDEX";

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    var total = System.getenv(ENV_SHARDING_TOTAL);
    var index = System.getenv(ENV_SHARDING_INDEX);

    if (ObjectUtils.isEmpty(total) || ObjectUtils.isEmpty(index)) {
      return ConditionEvaluationResult.enabled("Sharding disabled");
    }

    int totalShards = Integer.parseInt(total);
    int shardIndex = Integer.parseInt(index);

    if (context.getTestMethod().isEmpty()) {
      return ConditionEvaluationResult.enabled("Class-level evaluation, deferring to method level");
    }

    var testIdentifier = context.getUniqueId();
    int shardAssignment = Math.abs(testIdentifier.hashCode()) % totalShards;

    if (shardAssignment == shardIndex) {
      log.info("Running test {} (shard {})", testIdentifier, shardAssignment);
      return ConditionEvaluationResult.enabled("Assigned to current shard " + shardIndex);
    }

    log.info(
        "Skipping test {} (assigned to shard {}, current shard {})",
        testIdentifier,
        shardAssignment,
        shardIndex);
    return ConditionEvaluationResult.disabled(
        "Assigned to shard " + shardAssignment + ", current shard " + shardIndex);
  }
}
