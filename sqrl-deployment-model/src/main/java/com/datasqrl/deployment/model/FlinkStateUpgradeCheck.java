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
import com.datasqrl.deployment.model.StateUpgradeNote.ChangedFlinkVersion;
import com.datasqrl.deployment.model.StateUpgradeNote.ChangedStateTtl;
import com.datasqrl.deployment.model.StateUpgradeNote.MaxParallelismOnlyInNewBuild;
import com.datasqrl.deployment.model.StateUpgradeNote.MaxParallelismOnlyInOldBuild;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
 * new compiled plan with the same {@code id}, {@code type} and state fields. An operator with state
 * also needs the same column types in its {@code outputType}. An operator that only the new plan
 * has starts with empty state, so it is accepted. Both builds also need the same {@code
 * pipeline.max-parallelism}.
 */
public final class FlinkStateUpgradeCheck {

  /**
   * The operator fields that hold or shape the state, so they must stay the same. {@code joinSpec}
   * defines the state of a join that has no {@code state} field.
   */
  private static final List<String> STATE_FIELDS =
      List.of("state", "stateMetadata", "joinSpec", "inputProperties", "inputChangelogMode");

  private static final String STATE_FIELD = "state";

  private static final String JOIN_SPEC_FIELD = "joinSpec";

  /**
   * The field with the row type of an operator. An operator with state stores rows of this type, so
   * a new, removed or changed column makes the restore fail. Flink ignores the column names.
   */
  private static final String OUTPUT_TYPE_FIELD = "outputType";

  /** The field of a {@code state} entry that holds the TTL, for example {@code "1 h"}. */
  private static final String TTL_FIELD = "ttl";

  private static final String MISSING_VALUE = "missing";

  private FlinkStateUpgradeCheck() {}

  /** Returns all breaks and notes for an upgrade from the old build to the new build. */
  public static StateUpgradeResult check(FlinkJobPlan oldJobPlan, FlinkJobPlan newJobPlan) {
    var stateUpgradeBreaks =
        new ArrayList<>(operatorBreaks(oldJobPlan.compiledPlan(), newJobPlan.compiledPlan()));
    maxParallelismBreak(oldJobPlan.maxParallelism(), newJobPlan.maxParallelism())
        .ifPresent(stateUpgradeBreaks::add);

    var stateUpgradeNotes = new ArrayList<StateUpgradeNote>();
    changedFlinkVersion(oldJobPlan.compiledPlan(), newJobPlan.compiledPlan())
        .ifPresent(stateUpgradeNotes::add);
    stateUpgradeNotes.addAll(
        changedStateTtls(oldJobPlan.compiledPlan(), newJobPlan.compiledPlan()));
    maxParallelismOnlyInOneBuild(oldJobPlan.maxParallelism(), newJobPlan.maxParallelism())
        .ifPresent(stateUpgradeNotes::add);

    return new StateUpgradeResult(List.copyOf(stateUpgradeBreaks), List.copyOf(stateUpgradeNotes));
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
        var oldValue = fieldValue(oldOperator, fieldName);
        var newValue = fieldValue(newOperator, fieldName);
        if (!Objects.equals(
            comparableValue(fieldName, oldValue), comparableValue(fieldName, newValue))) {
          operatorBreaks.add(
              new ChangedStateField(
                  operatorId,
                  fieldName,
                  valueText(oldValue),
                  valueText(newValue),
                  description(oldOperator)));
        }
      }

      var oldOutputType = fieldValue(oldOperator, OUTPUT_TYPE_FIELD);
      var newOutputType = fieldValue(newOperator, OUTPUT_TYPE_FIELD);
      if (hasState(oldOperator)
          && !Objects.equals(columnTypes(oldOutputType), columnTypes(newOutputType))) {
        operatorBreaks.add(
            new ChangedStateField(
                operatorId,
                OUTPUT_TYPE_FIELD,
                outputTypeText(oldOutputType),
                outputTypeText(newOutputType),
                description(oldOperator)));
      }
    }
    return operatorBreaks;
  }

  /** An operator has state when it has a {@code state} field or a {@code joinSpec}. */
  private static boolean hasState(JsonNode operator) {
    return fieldValue(operator, STATE_FIELD) != null
        || fieldValue(operator, JOIN_SPEC_FIELD) != null;
  }

  /**
   * Returns the column types of a {@code ROW} output type in their order, without the column names,
   * because a renamed column does not change the stored rows. Returns the whole value for another
   * type, and {@code null} for a missing output type.
   */
  private static JsonNode columnTypes(JsonNode outputType) {
    if (outputType == null || !outputType.path("fields").isArray()) {
      return outputType;
    }
    var columnTypes = JsonNodeFactory.instance.arrayNode();
    outputType.path("fields").forEach(column -> columnTypes.add(column.path("fieldType")));
    return columnTypes;
  }

  /**
   * Returns the columns of a {@code ROW} output type as {@code [name type, ...]}, which is shorter
   * than the JSON. A type that is not plain text, such as a time attribute, stays compact JSON.
   */
  private static String outputTypeText(JsonNode outputType) {
    if (outputType == null || !outputType.path("fields").isArray()) {
      return valueText(outputType);
    }
    var columnTexts = new ArrayList<String>();
    outputType
        .path("fields")
        .forEach(
            column -> {
              var columnType = column.path("fieldType");
              columnTexts.add(
                  column.path("name").asText()
                      + " "
                      + (columnType.isTextual() ? columnType.asText() : columnType.toString()));
            });
    return "[" + String.join(", ", columnTexts) + "]";
  }

  /**
   * Returns one note for each state entry whose {@code ttl} is the only change. Flink restores the
   * state after a TTL change, so {@link #operatorBreaks} ignores the {@code ttl}. When other parts
   * of the {@code state} field change too, the break shows the full values instead.
   */
  private static List<ChangedStateTtl> changedStateTtls(
      JsonNode oldCompiledPlan, JsonNode newCompiledPlan) {
    var newOperatorsById = operatorsById(newCompiledPlan);
    var changedStateTtls = new ArrayList<ChangedStateTtl>();

    for (var oldOperator : oldCompiledPlan.path("nodes")) {
      var newOperator = newOperatorsById.get(operatorId(oldOperator));
      if (newOperator == null) {
        continue;
      }
      var oldStateEntries = fieldValue(oldOperator, STATE_FIELD);
      var newStateEntries = fieldValue(newOperator, STATE_FIELD);
      if (oldStateEntries == null
          || !oldStateEntries.isArray()
          || !Objects.equals(withoutTtl(oldStateEntries), withoutTtl(newStateEntries))) {
        continue;
      }

      for (var entryIndex = 0; entryIndex < oldStateEntries.size(); entryIndex++) {
        var oldTtl = oldStateEntries.get(entryIndex).path(TTL_FIELD).asText(MISSING_VALUE);
        var newTtl = newStateEntries.get(entryIndex).path(TTL_FIELD).asText(MISSING_VALUE);
        if (!oldTtl.equals(newTtl)) {
          changedStateTtls.add(
              new ChangedStateTtl(
                  operatorId(oldOperator),
                  oldStateEntries.get(entryIndex).path("name").asText(),
                  oldTtl,
                  newTtl,
                  description(oldOperator)));
        }
      }
    }
    return changedStateTtls;
  }

  /** Returns the value to compare: the {@code state} field without the {@code ttl} entries. */
  private static JsonNode comparableValue(String fieldName, JsonNode value) {
    return STATE_FIELD.equals(fieldName) ? withoutTtl(value) : value;
  }

  /** Returns a copy of a {@code state} array without the {@code ttl} of each entry. */
  private static JsonNode withoutTtl(JsonNode stateEntries) {
    if (stateEntries == null || !stateEntries.isArray()) {
      return stateEntries;
    }
    var stateEntriesWithoutTtl = stateEntries.deepCopy();
    stateEntriesWithoutTtl.forEach(
        stateEntry -> {
          if (stateEntry instanceof ObjectNode stateEntryObject) {
            stateEntryObject.remove(TTL_FIELD);
          }
        });
    return stateEntriesWithoutTtl;
  }

  /** Returns the field value as compact JSON, or {@code missing} for a missing field. */
  private static String valueText(JsonNode value) {
    return value == null ? MISSING_VALUE : value.toString();
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

  /** Returns a note when only one build sets {@code pipeline.max-parallelism}. */
  private static Optional<StateUpgradeNote> maxParallelismOnlyInOneBuild(
      Optional<Integer> oldMaxParallelism, Optional<Integer> newMaxParallelism) {
    if (oldMaxParallelism.isPresent() && newMaxParallelism.isEmpty()) {
      return Optional.of(new MaxParallelismOnlyInOldBuild(oldMaxParallelism.get()));
    }
    if (oldMaxParallelism.isEmpty() && newMaxParallelism.isPresent()) {
      return Optional.of(new MaxParallelismOnlyInNewBuild(newMaxParallelism.get()));
    }
    return Optional.empty();
  }

  /** Returns a note when both plans have a {@code flinkVersion} and the versions are different. */
  private static Optional<ChangedFlinkVersion> changedFlinkVersion(
      JsonNode oldCompiledPlan, JsonNode newCompiledPlan) {
    var oldFlinkVersion = oldCompiledPlan.path("flinkVersion").asText();
    var newFlinkVersion = newCompiledPlan.path("flinkVersion").asText();

    if (oldFlinkVersion.isEmpty()
        || newFlinkVersion.isEmpty()
        || oldFlinkVersion.equals(newFlinkVersion)) {
      return Optional.empty();
    }
    return Optional.of(new ChangedFlinkVersion(oldFlinkVersion, newFlinkVersion));
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
