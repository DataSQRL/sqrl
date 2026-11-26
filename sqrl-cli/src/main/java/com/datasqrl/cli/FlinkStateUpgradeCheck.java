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
package com.datasqrl.cli;

import com.datasqrl.cli.StateUpgradeBreak.OldState;
import com.datasqrl.error.ErrorCollector;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Checks if Flink can restore the state of an old build from a new build.
 *
 * <p>The operator check is one-way: each operator of the old compiled plan needs an operator in the
 * new compiled plan with the same {@code id}, {@code type} and state fields. An operator with state
 * also needs the same column types in its {@code outputType}. An operator that only the new plan
 * has starts with empty state, so it is accepted. Both builds also need the same {@code
 * pipeline.max-parallelism}.
 *
 * <p>A change that does not stop the restore goes to the {@link ErrorCollector} as a notice or a
 * warning: a new Flink version, a new TTL, and a {@code pipeline.max-parallelism} that only one
 * build sets.
 */
final class FlinkStateUpgradeCheck {

  /**
   * The operator fields that hold or shape the state, so they must stay the same. {@code joinSpec}
   * defines the state of a join that has no {@code state} field.
   */
  private static final List<String> STATE_FIELDS =
      List.of("state", "stateMetadata", "joinSpec", "inputProperties", "inputChangelogMode");

  private static final String STATE_FIELD = "state";

  private static final String JOIN_SPEC_FIELD = "joinSpec";

  private static final String SOURCE_KIND = "stream-exec-table-source-scan";

  private static final String SINK_KIND = "stream-exec-sink";

  /**
   * The operator kinds that keep no state. A local aggregate keeps its rows only in memory and
   * sends them on before each checkpoint. Every other kind counts as a kind with state, also
   * without a {@code state} field: Flink writes that field only for the operators with a TTL
   * setting, so a window aggregate or an interval join has no such field. A kind that a later Flink
   * version adds then also gets the checks for state. A source and a sink have their own rule, see
   * {@link #canHoldState}.
   */
  private static final Set<String> OPERATOR_KINDS_WITHOUT_STATE =
      Set.of(
          "stream-exec-calc",
          "stream-exec-correlate",
          "stream-exec-drop-update-before",
          "stream-exec-exchange",
          "stream-exec-expand",
          "stream-exec-local-group-aggregate",
          "stream-exec-local-window-aggregate",
          "stream-exec-mini-batch-assigner",
          "stream-exec-union",
          "stream-exec-values",
          "stream-exec-watermark-assigner");

  /**
   * The field with the row type of an operator. An operator with state stores rows of this type, so
   * a new, removed or changed column makes the restore fail. Flink ignores the column names.
   */
  private static final String OUTPUT_TYPE_FIELD = "outputType";

  /**
   * The field with the aggregate calls of an aggregate operator. Each function stores its own
   * accumulator, and Flink checks only the type of the stored value. So {@code SUM} changed to
   * {@code MAX} restores without an error, and the new function continues from the old sum. An over
   * aggregate keeps this field one level deeper, in {@code overSpec}.
   */
  private static final String AGG_CALLS_FIELD = "aggCalls";

  /**
   * {@code SUM0} keeps the same running total as {@code SUM}, and returns 0 in place of {@code
   * null} for no input. A restore from {@code SUM} into {@code SUM0} continues the old total, and
   * the DataSQRL planner can change one into the other, so the check treats them as one function.
   */
  private static final String SUM0_FUNCTION = "SUM0";

  private static final String SUM_FUNCTION = "SUM";

  /**
   * The names that Flink shows for the timestamp types of the object form of an {@code outputType}.
   */
  private static final Map<String, String> TIMESTAMP_TYPE_NAMES =
      Map.of(
          "TIMESTAMP_WITH_LOCAL_TIME_ZONE", "TIMESTAMP_LTZ",
          "TIMESTAMP_WITHOUT_TIME_ZONE", "TIMESTAMP");

  /** The field of a {@code state} entry that holds the TTL, for example {@code "1 h"}. */
  private static final String TTL_FIELD = "ttl";

  private static final String MISSING_VALUE = "missing";

  private FlinkStateUpgradeCheck() {}

  /**
   * The parts of a build that decide if Flink can restore the state of its job.
   *
   * @param compiledPlan the content of {@code flink-compiled-plan.json}
   * @param maxParallelism the {@code pipeline.max-parallelism} of {@code flink-config.yaml}, empty
   *     when the project does not set it
   */
  record FlinkJob(JsonNode compiledPlan, Optional<Integer> maxParallelism) {}

  /**
   * Returns all breaks for an upgrade from the old build to the new build, and reports the notices
   * and the warnings to the {@link ErrorCollector}.
   */
  static List<StateUpgradeBreak> check(
      FlinkJob oldFlinkJob, FlinkJob newFlinkJob, ErrorCollector errors) {
    var stateUpgradeBreaks =
        new ArrayList<>(operatorBreaks(oldFlinkJob.compiledPlan(), newFlinkJob.compiledPlan()));
    maxParallelismBreak(oldFlinkJob.maxParallelism(), newFlinkJob.maxParallelism())
        .ifPresent(stateUpgradeBreaks::add);

    reportChangedFlinkVersion(oldFlinkJob.compiledPlan(), newFlinkJob.compiledPlan(), errors);
    reportChangedStateTtls(oldFlinkJob.compiledPlan(), newFlinkJob.compiledPlan(), errors);
    reportMaxParallelismOnlyInOneBuild(
        oldFlinkJob.maxParallelism(), newFlinkJob.maxParallelism(), errors);

    return List.copyOf(stateUpgradeBreaks);
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

      var oldState = oldState(oldOperator, newOperator);
      var description = description(oldOperator);
      if (newOperator == null) {
        operatorBreaks.add(
            new StateUpgradeBreak(
                "operator id=%s removed: %s".formatted(operatorId, description), oldState));
        continue;
      }

      var oldType = type(oldOperator);
      var newType = type(newOperator);
      if (!oldType.equals(newType)) {
        operatorBreaks.add(
            changedField(operatorId, "type", oldType, newType, description, oldState));
      }

      for (var fieldName : STATE_FIELDS) {
        var oldValue = fieldValue(oldOperator, fieldName);
        var newValue = fieldValue(newOperator, fieldName);
        if (!Objects.equals(
            comparableValue(fieldName, oldValue), comparableValue(fieldName, newValue))) {
          operatorBreaks.add(
              changedField(
                  operatorId,
                  fieldName,
                  valueText(oldValue),
                  valueText(newValue),
                  description,
                  oldState));
        }
      }

      var oldOutputType = fieldValue(oldOperator, OUTPUT_TYPE_FIELD);
      var newOutputType = fieldValue(newOperator, OUTPUT_TYPE_FIELD);
      if (hasState(oldOperator)
          && !Objects.equals(columnTypes(oldOutputType), columnTypes(newOutputType))) {
        operatorBreaks.add(
            changedField(
                operatorId,
                OUTPUT_TYPE_FIELD,
                outputTypeText(oldOutputType),
                outputTypeText(newOutputType),
                description,
                oldState));
      }

      var oldAggregateFunctions = aggregateFunctions(oldOperator);
      var newAggregateFunctions = aggregateFunctions(newOperator);
      if (hasState(oldOperator) && !oldAggregateFunctions.equals(newAggregateFunctions)) {
        operatorBreaks.add(
            changedField(
                operatorId,
                AGG_CALLS_FIELD,
                oldAggregateFunctions.toString(),
                newAggregateFunctions.toString(),
                description,
                oldState));
      }
    }
    return operatorBreaks;
  }

  /**
   * Returns the break of a field that differs in the operator with this id. A different {@code
   * type} keeps the uid when only the ExecNode version changes, for example {@code
   * stream-exec-sink_1} to {@code stream-exec-sink_2}. The values are compact JSON, or {@code
   * missing} for a missing field. The description is the one of the old operator.
   */
  private static StateUpgradeBreak changedField(
      String operatorId,
      String fieldName,
      String oldValue,
      String newValue,
      String description,
      OldState oldState) {
    return new StateUpgradeBreak(
        "operator id=%s changed: field '%s' differs: %s -> %s (%s)"
            .formatted(operatorId, fieldName, oldValue, newValue, description),
        oldState);
  }

  /**
   * Returns the aggregate functions of an operator in their order, for example {@code [SUM, COUNT
   * DISTINCT]}. The list leaves out the column name and the input column positions: a renamed
   * column keeps the state, and the positions change when an earlier operator adds a column.
   */
  private static List<String> aggregateFunctions(JsonNode operator) {
    var aggregateFunctions = new ArrayList<String>();
    for (var aggCalls : operator.findValues(AGG_CALLS_FIELD)) {
      aggCalls.forEach(aggCall -> aggregateFunctions.add(aggregateFunction(aggCall)));
    }
    return aggregateFunctions;
  }

  /**
   * Returns the function of an aggregate call with its options, for example {@code COUNT DISTINCT}
   * for the {@code internalName} {@code "$COUNT$1"} with {@code distinct}. A call without an {@code
   * internalName}, for example a user-defined function, gives the call without its name and its
   * input column positions.
   */
  private static String aggregateFunction(JsonNode aggCall) {
    if (!aggCall.hasNonNull("internalName")) {
      var aggCallWithoutPositions = ((ObjectNode) aggCall.deepCopy());
      aggCallWithoutPositions.remove(List.of("name", "argList", "filterArg"));
      return aggCallWithoutPositions.toString();
    }
    var functionName =
        aggCall
            .path("internalName")
            .asText()
            .replaceFirst("^\\$+", "")
            .replaceFirst("\\$\\d+$", "");
    var aggregateFunction =
        new StringBuilder(functionName.equals(SUM0_FUNCTION) ? SUM_FUNCTION : functionName);
    if (aggCall.path("distinct").asBoolean()) {
      aggregateFunction.append(" DISTINCT");
    }
    if (aggCall.path("approximate").asBoolean()) {
      aggregateFunction.append(" APPROXIMATE");
    }
    if (aggCall.path("ignoreNulls").asBoolean()) {
      aggregateFunction.append(" IGNORE NULLS");
    }
    if (aggCall.path("filterArg").asInt(-1) >= 0) {
      aggregateFunction.append(" FILTER");
    }
    return aggregateFunction.toString();
  }

  /**
   * Decides what happens to the old state of an operator, the same for each break of the operator.
   * The old state is unclaimed when the operator can hold state, and the new plan has no operator
   * with its id, an operator of another kind at its id, or no {@code state} entry with the name of
   * an old entry. Otherwise the old state is incompatible when the operator has state (see {@link
   * #hasState}), and the operator has no state in the other cases.
   */
  private static OldState oldState(JsonNode oldOperator, JsonNode newOperator) {
    var oldStateIsUnclaimed =
        newOperator == null
            || !operatorKind(type(oldOperator)).equals(operatorKind(type(newOperator)))
            || !stateNames(fieldValue(newOperator, STATE_FIELD))
                .containsAll(stateNames(fieldValue(oldOperator, STATE_FIELD)));
    if (canHoldState(oldOperator) && oldStateIsUnclaimed) {
      return OldState.UNCLAIMED;
    }
    return hasState(oldOperator) ? OldState.INCOMPATIBLE : OldState.NONE;
  }

  /**
   * An operator can hold state when it has state (see {@link #hasState}), and also when it is a
   * source or a sink. A source keeps its read position, and a sink can keep the rows that it did
   * not write yet, but the plan does not show this state.
   */
  private static boolean canHoldState(JsonNode operator) {
    return hasState(operator) || isSourceOrSink(operator);
  }

  private static boolean isSourceOrSink(JsonNode operator) {
    var operatorKind = operatorKind(type(operator));
    return operatorKind.equals(SOURCE_KIND) || operatorKind.equals(SINK_KIND);
  }

  /**
   * Returns the kind of an operator type without its ExecNode version, for example {@code
   * stream-exec-sink} for {@code stream-exec-sink_2}. Flink matches the state of an operator by its
   * id and its kind, so a new version of the same kind still claims the old state.
   */
  private static String operatorKind(String type) {
    return type.replaceFirst("_\\d+$", "");
  }

  /** Returns the names of the entries of a {@code state} field, or no names for another value. */
  private static Set<String> stateNames(JsonNode stateEntries) {
    var stateNames = new HashSet<String>();
    if (stateEntries != null && stateEntries.isArray()) {
      stateEntries.forEach(stateEntry -> stateNames.add(stateEntry.path("name").asText()));
    }
    return stateNames;
  }

  /**
   * An operator has state when it has a {@code state} field or a {@code joinSpec}. An operator
   * without these fields has state when its kind is not in {@link #OPERATOR_KINDS_WITHOUT_STATE},
   * and when it is not a source or a sink. A source or a sink without a {@code state} field stores
   * no rows of its {@code outputType}, see {@link #canHoldState}.
   */
  private static boolean hasState(JsonNode operator) {
    if (fieldValue(operator, STATE_FIELD) != null
        || fieldValue(operator, JOIN_SPEC_FIELD) != null) {
      return true;
    }
    return !OPERATOR_KINDS_WITHOUT_STATE.contains(operatorKind(type(operator)))
        && !isSourceOrSink(operator);
  }

  /**
   * Returns the column types of a {@code ROW} output type in their order, without the column names,
   * because a renamed column does not change the stored rows. A compiled plan writes the type as an
   * object with {@code fields} when a column is a time attribute, and as text in the other cases.
   * Returns the whole value for another type, and {@code null} for a missing output type.
   */
  private static JsonNode columnTypes(JsonNode outputType) {
    if (outputType == null) {
      return null;
    }
    var columnTypes = JsonNodeFactory.instance.arrayNode();
    if (outputType.path("fields").isArray()) {
      outputType.path("fields").forEach(column -> columnTypes.add(column.path("fieldType")));
      return columnTypes;
    }
    if (outputType.isTextual()) {
      var columnTypeTexts = RowTypeText.columnTypes(outputType.asText());
      if (columnTypeTexts.isPresent()) {
        columnTypeTexts.get().forEach(columnTypes::add);
        return columnTypes;
      }
    }
    return outputType;
  }

  /**
   * Returns the columns of a {@code ROW} output type as {@code [name type, ...]}, in the same form
   * for the text form and the object form of the plan. Returns the value as it is when it is not a
   * {@code ROW} type. This method only builds the text of a break line: {@link #columnTypes} gives
   * the values that the check compares.
   */
  private static String outputTypeText(JsonNode outputType) {
    if (outputType != null && outputType.isTextual()) {
      return RowTypeText.columns(outputType.asText())
          .map(
              columns ->
                  columns.stream()
                      .map(column -> column.name() + " " + column.type())
                      .collect(Collectors.joining(", ", "[", "]")))
          .orElse(outputType.asText());
    }
    if (outputType == null || !outputType.path("fields").isArray()) {
      return valueText(outputType);
    }
    var columnTexts = new ArrayList<String>();
    outputType
        .path("fields")
        .forEach(
            column ->
                columnTexts.add(
                    column.path("name").asText() + " " + columnTypeText(column.path("fieldType"))));
    return "[" + String.join(", ", columnTexts) + "]";
  }

  /**
   * Returns the type of a column in the object form as text. A timestamp object, for example {@code
   * {"type":"TIMESTAMP_WITH_LOCAL_TIME_ZONE","nullable":false,"precision":3,"kind":"ROWTIME"}},
   * gives the text that Flink shows for it, for example {@code TIMESTAMP_LTZ(3) NOT NULL
   * *ROWTIME*}. Another object stays compact JSON.
   */
  private static String columnTypeText(JsonNode columnType) {
    if (columnType.isTextual()) {
      return columnType.asText();
    }
    var timestampTypeName = TIMESTAMP_TYPE_NAMES.get(columnType.path("type").asText());
    if (timestampTypeName == null || !columnType.has("precision")) {
      return columnType.toString();
    }
    var columnTypeText =
        new StringBuilder(timestampTypeName)
            .append('(')
            .append(columnType.path("precision").asInt())
            .append(')');
    if (!columnType.path("nullable").asBoolean(true)) {
      columnTypeText.append(" NOT NULL");
    }
    var timestampKind = columnType.path("kind").asText("REGULAR");
    if (!timestampKind.equals("REGULAR")) {
      columnTypeText.append(" *").append(timestampKind).append('*');
    }
    return columnTypeText.toString();
  }

  /**
   * Reports one notice for each state entry whose {@code ttl} is the only change. Flink restores
   * the state after a TTL change, so {@link #operatorBreaks} ignores the {@code ttl}. When other
   * parts of the {@code state} field change too, the break shows the full values instead. The level
   * is a notice, because a warning makes the agent change a good TTL back.
   */
  private static void reportChangedStateTtls(
      JsonNode oldCompiledPlan, JsonNode newCompiledPlan, ErrorCollector errors) {
    var newOperatorsById = operatorsById(newCompiledPlan);

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
          errors.notice(
              "operator id=%s: the TTL of state '%s' changes: %s -> %s (%s). The check does not"
                  + " report a TTL change as a break.",
              operatorId(oldOperator),
              oldStateEntries.get(entryIndex).path("name").asText(),
              oldTtl,
              newTtl,
              description(oldOperator));
        }
      }
    }
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

  /**
   * Returns a break when the two builds set different values for {@code pipeline.max-parallelism}.
   * Flink divides the state into this number of key groups, so a restore needs the same value.
   */
  private static Optional<StateUpgradeBreak> maxParallelismBreak(
      Optional<Integer> oldMaxParallelism, Optional<Integer> newMaxParallelism) {
    if (oldMaxParallelism.isPresent()
        && newMaxParallelism.isPresent()
        && !oldMaxParallelism.equals(newMaxParallelism)) {
      return Optional.of(
          new StateUpgradeBreak(
              "pipeline.max-parallelism differs: %d -> %d"
                  .formatted(oldMaxParallelism.get(), newMaxParallelism.get()),
              OldState.NONE));
    }
    return Optional.empty();
  }

  /**
   * Reports a warning when only one build sets {@code pipeline.max-parallelism}. When only the old
   * build sets it, Flink takes the value from the savepoint, so the restore fails only when the new
   * job gets a different value from outside the build. When only the new build sets it, the check
   * cannot see the value of the running job.
   */
  private static void reportMaxParallelismOnlyInOneBuild(
      Optional<Integer> oldMaxParallelism,
      Optional<Integer> newMaxParallelism,
      ErrorCollector errors) {
    if (oldMaxParallelism.isPresent() && newMaxParallelism.isEmpty()) {
      errors.warn(
          "Only the old build sets pipeline.max-parallelism (%d). Flink takes this value from the"
              + " savepoint. The restore fails only when the new job gets a different value from"
              + " outside the build, for example from the Flink cluster configuration.",
          oldMaxParallelism.get());
    }
    if (oldMaxParallelism.isEmpty() && newMaxParallelism.isPresent()) {
      errors.warn(
          "Only the new build sets pipeline.max-parallelism (%d). Make sure that the running job"
              + " uses the same value, because a different value makes the restore fail.",
          newMaxParallelism.get());
    }
  }

  /**
   * Reports a notice when both plans have a {@code flinkVersion} and the versions are different.
   * This is information only: a Flink upgrade that changes an operator also changes its {@code
   * type}, which the check reports as a break.
   */
  private static void reportChangedFlinkVersion(
      JsonNode oldCompiledPlan, JsonNode newCompiledPlan, ErrorCollector errors) {
    var oldFlinkVersion = oldCompiledPlan.path("flinkVersion").asText();
    var newFlinkVersion = newCompiledPlan.path("flinkVersion").asText();

    if (!oldFlinkVersion.isEmpty()
        && !newFlinkVersion.isEmpty()
        && !oldFlinkVersion.equals(newFlinkVersion)) {
      errors.notice("The Flink version changes: %s -> %s", oldFlinkVersion, newFlinkVersion);
    }
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
