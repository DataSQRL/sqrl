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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions.UidGeneration;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

/**
 * Checks if Flink can restore the state of an old build from a new build.
 *
 * <p>The operator check is one-way: each operator of the old compiled plan needs an operator in the
 * new compiled plan with the same {@code id}, {@code type} and state fields. An operator with state
 * also needs the same column types in its {@code outputType}. An operator that only the new plan
 * has starts with empty state, so it is accepted. Both builds also need the same {@code
 * pipeline.max-parallelism} and must give the operators the same uids.
 *
 * <p>A change that does not stop the restore goes to the {@link ErrorCollector} as a notice or a
 * warning: a new Flink version, a new TTL, a {@code pipeline.max-parallelism} that only one build
 * sets, and two builds that let Flink make the uids.
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
   * The window table function of a {@code TUMBLE}, {@code HOP} or {@code CUMULATE} window without
   * an aggregate only adds the window columns to each row, and keeps no state. A {@code SESSION}
   * window merges the windows of a key, so it keeps state.
   */
  private static final String WINDOW_TABLE_FUNCTION_KIND = "stream-exec-window-table-function";

  /** The {@code windowing.window.type} values of the windows that keep no state. */
  private static final Set<String> ALIGNED_WINDOW_TYPES =
      Set.of("TumblingWindow", "HoppingWindow", "CumulativeWindow");

  /**
   * The field with the row type of an operator. An operator with state stores rows of this type, so
   * a new, removed or changed column makes the restore fail. Flink ignores the column names.
   */
  private static final String OUTPUT_TYPE_FIELD = "outputType";

  /**
   * The operator kinds that run a function asynchronously. Flink stores the input rows that wait
   * for the answer of the function, not the rows of the {@code outputType}: {@code
   * AsyncWaitOperator} writes its queue with the serializer of its input. So a new return type of
   * the function keeps the stored rows, and a new input column makes the restore fail.
   */
  private static final Set<String> ASYNC_OPERATOR_KINDS =
      Set.of("stream-exec-async-calc", "stream-exec-async-correlate");

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
   * @param uidFormat the {@code table.exec.uid.format} of {@code flink-config.yaml}, or the Flink
   *     default {@code <id>_<transformation>}
   * @param uidGeneration the {@code table.exec.uid.generation} of {@code flink-config.yaml}, or the
   *     Flink default {@code PLAN_ONLY}
   */
  record FlinkJob(
      JsonNode compiledPlan,
      Optional<Integer> maxParallelism,
      String uidFormat,
      UidGeneration uidGeneration) {}

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
    uidBreak(oldFlinkJob, newFlinkJob).ifPresent(stateUpgradeBreaks::add);

    reportChangedFlinkVersion(oldFlinkJob.compiledPlan(), newFlinkJob.compiledPlan(), errors);
    reportChangedStateTtls(oldFlinkJob.compiledPlan(), newFlinkJob.compiledPlan(), errors);
    reportMaxParallelismOnlyInOneBuild(
        oldFlinkJob.maxParallelism(), newFlinkJob.maxParallelism(), errors);
    reportUidsFromJobGraph(oldFlinkJob, newFlinkJob, errors);

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

      if (hasState(oldOperator)) {
        storedRowTypeBreak(
                oldCompiledPlan, oldOperator, newCompiledPlan, newOperator, description, oldState)
            .ifPresent(operatorBreaks::add);
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
   * Returns the break when the column types of the rows that an operator with state stores differ.
   * An async operator stores its input rows, see {@link #ASYNC_OPERATOR_KINDS}, so the check
   * compares the {@code outputType} of the operator that feeds it. Each other operator stores rows
   * of its own {@code outputType}. When the plan has no edge to the input of an async operator, the
   * check compares its {@code outputType}, so that a change of the rows still gives a break.
   */
  private static Optional<StateUpgradeBreak> storedRowTypeBreak(
      JsonNode oldCompiledPlan,
      JsonNode oldOperator,
      JsonNode newCompiledPlan,
      JsonNode newOperator,
      String description,
      OldState oldState) {
    var operatorId = operatorId(oldOperator);
    var oldInputType = asyncInputType(oldCompiledPlan, oldOperator);
    var newInputType = asyncInputType(newCompiledPlan, newOperator);
    if (oldInputType.isPresent() && newInputType.isPresent()) {
      if (Objects.equals(columnTypes(oldInputType.get()), columnTypes(newInputType.get()))) {
        return Optional.empty();
      }
      return Optional.of(
          new StateUpgradeBreak(
              "operator id=%s changed: the input rows that the operator stores differ: %s -> %s (%s)"
                  .formatted(
                      operatorId,
                      outputTypeText(oldInputType.get()),
                      outputTypeText(newInputType.get()),
                      description),
              oldState));
    }

    var oldOutputType = fieldValue(oldOperator, OUTPUT_TYPE_FIELD);
    var newOutputType = fieldValue(newOperator, OUTPUT_TYPE_FIELD);
    if (Objects.equals(columnTypes(oldOutputType), columnTypes(newOutputType))) {
      return Optional.empty();
    }
    return Optional.of(
        changedField(
            operatorId,
            OUTPUT_TYPE_FIELD,
            outputTypeText(oldOutputType),
            outputTypeText(newOutputType),
            description,
            oldState));
  }

  /**
   * Returns the {@code outputType} of the operator that feeds an async operator, from the {@code
   * edges} of the compiled plan. Returns empty for another kind of operator, and when the plan has
   * no single edge to the operator or no {@code outputType} at its source.
   */
  private static Optional<JsonNode> asyncInputType(JsonNode compiledPlan, JsonNode operator) {
    if (!ASYNC_OPERATOR_KINDS.contains(operatorKind(type(operator)))) {
      return Optional.empty();
    }
    var operatorId = operatorId(operator);
    var inputOperatorIds = new ArrayList<String>();
    for (var edge : compiledPlan.path("edges")) {
      if (edge.path("target").asText().equals(operatorId)) {
        inputOperatorIds.add(edge.path("source").asText());
      }
    }
    if (inputOperatorIds.size() != 1) {
      return Optional.empty();
    }
    return Optional.ofNullable(operatorsById(compiledPlan).get(inputOperatorIds.get(0)))
        .map(inputOperator -> fieldValue(inputOperator, OUTPUT_TYPE_FIELD));
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
   * no rows of its {@code outputType}, see {@link #canHoldState}. A window table function has state
   * only for a window that is not in {@link #ALIGNED_WINDOW_TYPES}, such as a session window.
   */
  private static boolean hasState(JsonNode operator) {
    if (fieldValue(operator, STATE_FIELD) != null
        || fieldValue(operator, JOIN_SPEC_FIELD) != null) {
      return true;
    }
    var operatorKind = operatorKind(type(operator));
    if (operatorKind.equals(WINDOW_TABLE_FUNCTION_KIND)) {
      return !ALIGNED_WINDOW_TYPES.contains(
          operator.path("windowing").path("window").path("type").asText());
    }
    return !OPERATOR_KINDS_WITHOUT_STATE.contains(operatorKind) && !isSourceOrSink(operator);
  }

  /**
   * Returns the column types of a {@code ROW} output type in their order, without the column names.
   * This is the rule of Flink for stored rows: {@code RowDataSerializer} restores when the {@code
   * LogicalType} of each column is {@code equals}. That equality ignores the column names and the
   * time attribute of a timestamp, and it keeps the names of a nested {@code ROW}. Returns the
   * value as it is when Flink cannot read the type, and {@code null} for a missing output type.
   */
  private static Object columnTypes(JsonNode outputType) {
    if (outputType == null) {
      return null;
    }
    return rowType(outputType).<Object>map(RowType::getChildren).orElse(outputType);
  }

  /**
   * Returns the columns of a {@code ROW} output type as {@code [name type, ...]}, in the same form
   * for both forms of the plan. Returns the value as it is when Flink cannot read the type. This
   * method only builds the text of a break line: {@link #columnTypes} gives the values that the
   * check compares.
   */
  private static String outputTypeText(JsonNode outputType) {
    if (outputType == null) {
      return MISSING_VALUE;
    }
    return rowType(outputType)
        .map(
            rowType ->
                rowType.getFields().stream()
                    .map(field -> field.getName() + " " + field.getType().asSummaryString())
                    .collect(Collectors.joining(", ", "[", "]")))
        .orElseGet(() -> outputType.isTextual() ? outputType.asText() : outputType.toString());
  }

  /**
   * Reads a {@code ROW} output type of the compiled plan. Flink writes it as the text of {@link
   * LogicalType#asSerializableString}, or, when a column is a time attribute, as an object with
   * {@code fields}, where the time attribute is an object such as {@code
   * {"type":"TIMESTAMP_WITH_LOCAL_TIME_ZONE","nullable":false,"precision":3,"kind":"ROWTIME"}}.
   * Returns empty for another type, and for a type that Flink cannot read, for example a {@code
   * RAW} type whose class is not on the class path of the CLI.
   */
  private static Optional<RowType> rowType(JsonNode outputType) {
    try {
      if (!outputType.path("fields").isArray()) {
        return Optional.of(parseType(outputType.asText()))
            .filter(RowType.class::isInstance)
            .map(RowType.class::cast);
      }
      var fields = new ArrayList<RowType.RowField>();
      for (var field : outputType.path("fields")) {
        fields.add(
            new RowType.RowField(field.path("name").asText(), fieldType(field.path("fieldType"))));
      }
      return Optional.of(new RowType(fields));
    } catch (ValidationException | IllegalArgumentException exception) {
      return Optional.empty();
    }
  }

  private static LogicalType fieldType(JsonNode fieldType) {
    if (fieldType.isTextual()) {
      return parseType(fieldType.asText());
    }
    var nullable = fieldType.path("nullable").asBoolean(true);
    var kind = TimestampKind.valueOf(fieldType.path("kind").asText(TimestampKind.REGULAR.name()));
    var precision = fieldType.path("precision").asInt();
    return switch (fieldType.path("type").asText()) {
      case "TIMESTAMP_WITH_LOCAL_TIME_ZONE" ->
          new LocalZonedTimestampType(nullable, kind, precision);
      case "TIMESTAMP_WITHOUT_TIME_ZONE" -> new TimestampType(nullable, kind, precision);
      default -> throw new IllegalArgumentException("Unknown field type: " + fieldType);
    };
  }

  private static LogicalType parseType(String typeText) {
    return LogicalTypeParser.parse(typeText, FlinkStateUpgradeCheck.class.getClassLoader());
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
   * Returns a break when the two builds give the operators different uids. Flink finds the old
   * state of an operator by its uid, so with other uids no operator claims the old state. For a job
   * that runs a compiled plan, {@code PLAN_ONLY} and {@code ALWAYS} set the same uids from {@code
   * table.exec.uid.format}. {@code DISABLED} sets no uids, so Flink makes the uids from the shape
   * of the job graph.
   */
  private static Optional<StateUpgradeBreak> uidBreak(FlinkJob oldFlinkJob, FlinkJob newFlinkJob) {
    var oldBuildSetsUids = setsUids(oldFlinkJob);
    var newBuildSetsUids = setsUids(newFlinkJob);
    if (oldBuildSetsUids != newBuildSetsUids) {
      return Optional.of(
          uidOptionBreak(
              ExecutionConfigOptions.TABLE_EXEC_UID_GENERATION.key(),
              oldFlinkJob.uidGeneration().name(),
              newFlinkJob.uidGeneration().name()));
    }
    if (oldBuildSetsUids && !oldFlinkJob.uidFormat().equals(newFlinkJob.uidFormat())) {
      return Optional.of(
          uidOptionBreak(
              ExecutionConfigOptions.TABLE_EXEC_UID_FORMAT.key(),
              oldFlinkJob.uidFormat(),
              newFlinkJob.uidFormat()));
    }
    return Optional.empty();
  }

  private static boolean setsUids(FlinkJob flinkJob) {
    return flinkJob.uidGeneration() != UidGeneration.DISABLED;
  }

  private static StateUpgradeBreak uidOptionBreak(
      String optionKey, String oldValue, String newValue) {
    return new StateUpgradeBreak(
        "%s differs: %s -> %s, so Flink gives each operator another uid"
            .formatted(optionKey, oldValue, newValue),
        OldState.UNCLAIMED);
  }

  /**
   * Reports a warning when both builds set {@code table.exec.uid.generation} to {@code DISABLED}.
   * Flink then makes the uids from the shape of the job graph, and the check cannot see these uids.
   */
  private static void reportUidsFromJobGraph(
      FlinkJob oldFlinkJob, FlinkJob newFlinkJob, ErrorCollector errors) {
    if (!setsUids(oldFlinkJob) && !setsUids(newFlinkJob)) {
      errors.warn(
          "Both builds set %s to %s, so Flink makes the operator uids from the shape of the job"
              + " graph. The check cannot see these uids, so it cannot decide if each operator gets"
              + " its old state.",
          ExecutionConfigOptions.TABLE_EXEC_UID_GENERATION.key(), UidGeneration.DISABLED.name());
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
