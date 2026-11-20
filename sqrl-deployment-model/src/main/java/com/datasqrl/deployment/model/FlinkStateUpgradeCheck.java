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
package com.datasqrl.deployment.model;

import com.datasqrl.deployment.model.StateUpgradeBreak.ChangedMaxParallelism;
import com.datasqrl.deployment.model.StateUpgradeBreak.ChangedOperatorType;
import com.datasqrl.deployment.model.StateUpgradeBreak.ChangedStateField;
import com.datasqrl.deployment.model.StateUpgradeBreak.RemovedOperator;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Checks if Flink can restore the state of an old build from a new build.
 *
 * <p>The operator check is one-way: each operator of the old compiled plan needs an operator in the
 * new compiled plan with the same {@code id}, {@code type} and state fields. An operator that only
 * the new plan has starts with empty state, so it is accepted. Both builds also need the same
 * {@code pipeline.max-parallelism}.
 */
public final class FlinkStateUpgradeCheck {

  /**
   * The operator fields that hold or shape the state, so they must stay the same. {@code joinSpec}
   * defines the state of a join that has no {@code state} field.
   */
  private static final List<String> STATE_FIELDS =
      List.of("state", "stateMetadata", "joinSpec", "inputProperties", "inputChangelogMode");

  private FlinkStateUpgradeCheck() {}

  /** Returns all breaks and warnings for an upgrade from the old build to the new build. */
  public static StateUpgradeResult check(FlinkJobPlan oldJobPlan, FlinkJobPlan newJobPlan) {
    var stateUpgradeBreaks =
        new ArrayList<>(operatorBreaks(oldJobPlan.compiledPlan(), newJobPlan.compiledPlan()));
    maxParallelismBreak(oldJobPlan.maxParallelism(), newJobPlan.maxParallelism())
        .ifPresent(stateUpgradeBreaks::add);

    return new StateUpgradeResult(
        List.copyOf(stateUpgradeBreaks),
        maxParallelismWarning(oldJobPlan.maxParallelism(), newJobPlan.maxParallelism()).stream()
            .toList(),
        flinkVersionChange(oldJobPlan.compiledPlan(), newJobPlan.compiledPlan()));
  }

  /**
   * Returns one break for each operator of the old plan that the new plan removes, and for each
   * field that the new plan changes, in the order of the old plan. A plan without {@code nodes} has
   * no operators.
   */
  private static List<StateUpgradeBreak> operatorBreaks(
      JsonNode oldCompiledPlan, JsonNode newCompiledPlan) {
    var newOperatorsById = operatorsById(newCompiledPlan);
    var operatorBreaks = new ArrayList<StateUpgradeBreak>();

    for (var oldOperator : oldCompiledPlan.path("nodes")) {
      var operatorId = operatorId(oldOperator);
      var newOperator = newOperatorsById.get(operatorId);

      if (newOperator == null) {
        operatorBreaks.add(new RemovedOperator(operatorId, description(oldOperator)));
        continue;
      }

      var oldType = type(oldOperator);
      var newType = type(newOperator);
      if (!oldType.equals(newType)) {
        operatorBreaks.add(
            new ChangedOperatorType(operatorId, oldType, newType, description(oldOperator)));
      }

      for (var fieldName : STATE_FIELDS) {
        if (!Objects.equals(
            fieldValue(oldOperator, fieldName), fieldValue(newOperator, fieldName))) {
          operatorBreaks.add(
              new ChangedStateField(
                  operatorId, fieldName, description(oldOperator), description(newOperator)));
        }
      }
    }
    return operatorBreaks;
  }

  private static Optional<StateUpgradeBreak> maxParallelismBreak(
      Optional<Integer> oldMaxParallelism, Optional<Integer> newMaxParallelism) {
    if (oldMaxParallelism.isPresent()
        && newMaxParallelism.isPresent()
        && !oldMaxParallelism.equals(newMaxParallelism)) {
      return Optional.of(
          new ChangedMaxParallelism(oldMaxParallelism.get(), newMaxParallelism.get()));
    }
    return Optional.empty();
  }

  /**
   * Returns a warning when only one build sets {@code pipeline.max-parallelism}. Flink then
   * computes the value of the other build, so the check cannot decide if the values are the same.
   */
  private static Optional<String> maxParallelismWarning(
      Optional<Integer> oldMaxParallelism, Optional<Integer> newMaxParallelism) {
    if (oldMaxParallelism.isPresent() && newMaxParallelism.isEmpty()) {
      return Optional.of(
          ("Only the old build sets pipeline.max-parallelism (%d). Set the same value for the new"
                  + " build, because a different value makes the restore fail.")
              .formatted(oldMaxParallelism.get()));
    }
    if (oldMaxParallelism.isEmpty() && newMaxParallelism.isPresent()) {
      return Optional.of(
          ("Only the new build sets pipeline.max-parallelism (%d). Make sure that the running job"
                  + " uses the same value, because a different value makes the restore fail.")
              .formatted(newMaxParallelism.get()));
    }
    return Optional.empty();
  }

  /**
   * Returns {@code "<old> -> <new>"} when both plans have a {@code flinkVersion} and the versions
   * are different. This is information only: a Flink upgrade that changes an operator also changes
   * its {@code type}, which {@link #operatorBreaks} reports.
   */
  private static Optional<String> flinkVersionChange(
      JsonNode oldCompiledPlan, JsonNode newCompiledPlan) {
    var oldFlinkVersion = oldCompiledPlan.path("flinkVersion").asText();
    var newFlinkVersion = newCompiledPlan.path("flinkVersion").asText();

    if (oldFlinkVersion.isEmpty()
        || newFlinkVersion.isEmpty()
        || oldFlinkVersion.equals(newFlinkVersion)) {
      return Optional.empty();
    }
    return Optional.of(oldFlinkVersion + " -> " + newFlinkVersion);
  }

  private static Map<String, JsonNode> operatorsById(JsonNode plan) {
    var operatorsById = new HashMap<String, JsonNode>();
    for (var operator : plan.path("nodes")) {
      operatorsById.put(operatorId(operator), operator);
    }
    return operatorsById;
  }

  /** The id as text, so that the number {@code 1} and the string {@code "1"} are the same id. */
  private static String operatorId(JsonNode operator) {
    return operator.path("id").asText();
  }

  /** Returns {@code null} for a missing field and for a JSON {@code null}, so both are equal. */
  private static JsonNode fieldValue(JsonNode operator, String fieldName) {
    var value = operator.get(fieldName);
    return value == null || value.isNull() ? null : value;
  }

  /** The type includes the ExecNode version, for example {@code stream-exec-sink_1}. */
  private static String type(JsonNode operator) {
    return operator.path("type").asText();
  }

  private static String description(JsonNode operator) {
    return operator.path("description").asText();
  }
}
