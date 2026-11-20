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
 * Checks if a new compiled Flink plan ({@code flink-compiled-plan.json}) can restore the state of
 * an old one.
 *
 * <p>The check is one-way: each operator of the old plan needs an operator in the new plan with the
 * same {@code id}, {@code type} and state fields. An operator that only the new plan has starts
 * with empty state, so it is accepted.
 */
public final class FlinkStateUpgradeCheck {

  /**
   * The operator fields that hold or shape the state, so they must stay the same. {@code joinSpec}
   * defines the state of a join that has no {@code state} field.
   */
  private static final List<String> STATE_FIELDS =
      List.of("state", "stateMetadata", "joinSpec", "inputProperties", "inputChangelogMode");

  public enum MaxParallelismResult {
    SAME,
    DIFFERENT,
    ONLY_OLD_SET,
    ONLY_NEW_SET,
    NOT_SET
  }

  private FlinkStateUpgradeCheck() {}

  /**
   * Returns one break for each operator of the old plan that the new plan removes, and for each
   * field that the new plan changes, in the order of the old plan. A plan without {@code nodes} has
   * no operators.
   */
  public static List<StateUpgradeBreak> findBreaks(JsonNode oldPlan, JsonNode newPlan) {
    var newOperatorsById = operatorsById(newPlan);
    var stateUpgradeBreaks = new ArrayList<StateUpgradeBreak>();

    for (var oldOperator : oldPlan.path("nodes")) {
      var operatorId = operatorId(oldOperator);
      var newOperator = newOperatorsById.get(operatorId);

      if (newOperator == null) {
        stateUpgradeBreaks.add(new RemovedOperator(operatorId, description(oldOperator)));
        continue;
      }

      var oldType = type(oldOperator);
      var newType = type(newOperator);
      if (!oldType.equals(newType)) {
        stateUpgradeBreaks.add(
            new ChangedOperatorType(operatorId, oldType, newType, description(oldOperator)));
      }

      for (var fieldName : STATE_FIELDS) {
        if (!Objects.equals(
            fieldValue(oldOperator, fieldName), fieldValue(newOperator, fieldName))) {
          stateUpgradeBreaks.add(
              new ChangedStateField(
                  operatorId, fieldName, description(oldOperator), description(newOperator)));
        }
      }
    }
    return stateUpgradeBreaks;
  }

  /**
   * Returns {@code "<old> -> <new>"} when both plans have a {@code flinkVersion} and the versions
   * are different. This is information only: a Flink upgrade that changes an operator also changes
   * its {@code type}, which {@link #findBreaks} reports.
   */
  public static Optional<String> flinkVersionChange(JsonNode oldPlan, JsonNode newPlan) {
    var oldFlinkVersion = oldPlan.path("flinkVersion").asText();
    var newFlinkVersion = newPlan.path("flinkVersion").asText();

    if (oldFlinkVersion.isEmpty()
        || newFlinkVersion.isEmpty()
        || oldFlinkVersion.equals(newFlinkVersion)) {
      return Optional.empty();
    }
    return Optional.of(oldFlinkVersion + " -> " + newFlinkVersion);
  }

  /** Compares the {@code pipeline.max-parallelism} values of two builds. */
  public static MaxParallelismResult compareMaxParallelism(
      Optional<Integer> oldValue, Optional<Integer> newValue) {
    if (oldValue.isPresent() && newValue.isPresent()) {
      return oldValue.equals(newValue) ? MaxParallelismResult.SAME : MaxParallelismResult.DIFFERENT;
    }
    if (oldValue.isPresent()) {
      return MaxParallelismResult.ONLY_OLD_SET;
    }
    if (newValue.isPresent()) {
      return MaxParallelismResult.ONLY_NEW_SET;
    }
    return MaxParallelismResult.NOT_SET;
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
