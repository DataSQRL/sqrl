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

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.deployment.model.StateUpgradeBreak.ChangedMaxParallelism;
import com.datasqrl.deployment.model.StateUpgradeBreak.ChangedOperatorType;
import com.datasqrl.deployment.model.StateUpgradeBreak.ChangedStateField;
import com.datasqrl.deployment.model.StateUpgradeBreak.RemovedOperator;
import com.datasqrl.deployment.model.StateUpgradeNote.ChangedFlinkVersion;
import com.datasqrl.deployment.model.StateUpgradeNote.ChangedStateTtl;
import com.datasqrl.deployment.model.StateUpgradeNote.Level;
import com.datasqrl.deployment.model.StateUpgradeNote.MaxParallelismOnlyInNewBuild;
import com.datasqrl.deployment.model.StateUpgradeNote.MaxParallelismOnlyInOldBuild;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkStateUpgradeCheckTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  void givenSamePlan_whenCheck_thenNoBreaks() {
    var result =
        check(
            plan("2.3", sourceOperator(), aggregateOperator(), sinkOperator()),
            plan("2.3", sourceOperator(), aggregateOperator(), sinkOperator()));

    assertThat(result.breaks()).isEmpty();
    assertThat(result.notes()).isEmpty();
  }

  @Test
  void givenOperatorOnlyInNewPlan_whenCheck_thenNoBreaks() {
    var result =
        check(
            plan("2.3", sourceOperator(), aggregateOperator()),
            plan("2.3", sourceOperator(), aggregateOperator(), sinkOperator()));

    assertThat(result.breaks()).isEmpty();
  }

  @Test
  void givenOperatorMissingInNewPlan_whenCheck_thenRemovedOperator() {
    var result =
        check(
            plan("2.3", sourceOperator(), aggregateOperator(), sinkOperator()),
            plan("2.3", sourceOperator(), sinkOperator()));

    assertThat(result.breaks())
        .containsExactly(new RemovedOperator("2", "GroupAggregate(groupBy=[customerid])"));
    assertThat(result.breaks().get(0).toMessage())
        .isEqualTo("operator id=2 removed: GroupAggregate(groupBy=[customerid])");
  }

  @Test
  void givenTypeChanged_whenCheck_thenChangedOperatorType() {
    var result =
        check(
            plan("2.3", sinkOperator()),
            plan("2.3", sinkOperator().put("type", "stream-exec-sink_2")));

    assertThat(result.breaks())
        .containsExactly(
            new ChangedOperatorType(
                "3", "stream-exec-sink_1", "stream-exec-sink_2", "Sink(table=[CustomerCount])"));
    assertThat(result.breaks().get(0).toMessage())
        .isEqualTo(
            "operator id=3 changed: field 'type' differs:"
                + " stream-exec-sink_1 -> stream-exec-sink_2 (Sink(table=[CustomerCount]))");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"state", "stateMetadata", "joinSpec", "inputProperties", "inputChangelogMode"})
  void givenStateFieldChanged_whenCheck_thenChangedStateFieldOnThatField(String fieldName) {
    var result =
        check(plan("2.3", joinOperator()), plan("2.3", joinOperator().put(fieldName, "changed")));

    assertThat(result.breaks())
        .containsExactly(
            new ChangedStateField(
                "4", fieldName, "missing", "\"changed\"", "Join(joinType=[InnerJoin])"));
  }

  @Test
  void givenStateChanged_whenCheck_thenMessageShowsOldAndNewValue() {
    var result =
        check(
            plan("2.3", aggregateOperator()),
            plan("2.3", aggregateOperator().put("state", "otherState")));

    assertThat(result.breaks()).hasSize(1);
    assertThat(result.breaks().get(0).toMessage())
        .isEqualTo(
            "operator id=2 changed: field 'state' differs:"
                + " \"groupAggregateState\" -> \"otherState\""
                + " (GroupAggregate(groupBy=[customerid]))");
  }

  @Test
  void givenOnlyStateTtlChanged_whenCheck_thenNoBreaksAndChangedStateTtl() {
    var result =
        check(plan("2.3", deduplicateOperator("0 ms")), plan("2.3", deduplicateOperator("1 h")));

    assertThat(result.breaks()).isEmpty();
    assertThat(result.notes())
        .containsExactly(
            new ChangedStateTtl(
                "5", "deduplicateState", "0 ms", "1 h", "Deduplicate(keep=[LastRow], key=[id])"));
    assertThat(result.notes().get(0).level()).isEqualTo(Level.NOTICE);
    assertThat(result.notes().get(0).toMessage())
        .isEqualTo(
            "operator id=5: the TTL of state 'deduplicateState' changes: 0 ms -> 1 h"
                + " (Deduplicate(keep=[LastRow], key=[id])). The check does not report a TTL"
                + " change as a break.");
  }

  @Test
  void givenNewColumnOnOperatorWithState_whenCheck_thenChangedOutputType() {
    var result =
        check(
            plan("2.3", withOutputType(deduplicateOperator("0 ms"), "id INT", "val BIGINT")),
            plan(
                "2.3",
                withOutputType(deduplicateOperator("0 ms"), "id INT", "val BIGINT", "extra INT")));

    assertThat(result.breaks())
        .containsExactly(
            new ChangedStateField(
                "5",
                "outputType",
                "[id INT, val BIGINT]",
                "[id INT, val BIGINT, extra INT]",
                "Deduplicate(keep=[LastRow], key=[id])"));
    assertThat(result.breaks().get(0).toMessage())
        .isEqualTo(
            "operator id=5 changed: field 'outputType' differs: [id INT, val BIGINT] ->"
                + " [id INT, val BIGINT, extra INT] (Deduplicate(keep=[LastRow], key=[id]))");
  }

  @ParameterizedTest
  @ValueSource(strings = {"BIGINT NOT NULL", "INT"})
  void givenChangedColumnTypeOnOperatorWithState_whenCheck_thenChangedOutputType(
      String newColumnType) {
    var result =
        check(
            plan("2.3", withOutputType(deduplicateOperator("0 ms"), "id INT", "val BIGINT")),
            plan(
                "2.3",
                withOutputType(deduplicateOperator("0 ms"), "id INT", "val " + newColumnType)));

    assertThat(result.breaks())
        .singleElement()
        .isEqualTo(
            new ChangedStateField(
                "5",
                "outputType",
                "[id INT, val BIGINT]",
                "[id INT, val " + newColumnType + "]",
                "Deduplicate(keep=[LastRow], key=[id])"));
  }

  @Test
  void givenRenamedColumnOnOperatorWithState_whenCheck_thenNoBreaks() {
    var result =
        check(
            plan("2.3", withOutputType(deduplicateOperator("0 ms"), "id INT", "val BIGINT")),
            plan("2.3", withOutputType(deduplicateOperator("0 ms"), "id INT", "amount BIGINT")));

    assertThat(result.breaks()).isEmpty();
  }

  @Test
  void givenNewColumnOnJoinWithJoinSpec_whenCheck_thenChangedOutputType() {
    var oldJoinOperator = withOutputType(joinOperator(), "id INT");
    oldJoinOperator.putObject("joinSpec").put("joinType", "INNER");
    var newJoinOperator = withOutputType(joinOperator(), "id INT", "extra INT");
    newJoinOperator.putObject("joinSpec").put("joinType", "INNER");

    var result = check(plan("2.3", oldJoinOperator), plan("2.3", newJoinOperator));

    assertThat(result.breaks())
        .singleElement()
        .extracting(stateUpgradeBreak -> ((ChangedStateField) stateUpgradeBreak).fieldName())
        .isEqualTo("outputType");
  }

  @Test
  void givenNewColumnOnOperatorWithoutState_whenCheck_thenNoBreaks() {
    var result =
        check(
            plan("2.3", withOutputType(sourceOperator(), "id INT")),
            plan("2.3", withOutputType(sourceOperator(), "id INT", "extra INT")));

    assertThat(result.breaks()).isEmpty();
  }

  @Test
  void givenStateNameAndTtlChanged_whenCheck_thenChangedStateFieldWithFullValues() {
    var newDeduplicateOperator = deduplicateOperator("1 h");
    ((ObjectNode) newDeduplicateOperator.get("state").get(0)).put("name", "otherState");

    var result =
        check(plan("2.3", deduplicateOperator("0 ms")), plan("2.3", newDeduplicateOperator));

    assertThat(result.notes()).isEmpty();
    assertThat(result.breaks())
        .containsExactly(
            new ChangedStateField(
                "5",
                "state",
                "[{\"index\":0,\"ttl\":\"0 ms\",\"name\":\"deduplicateState\"}]",
                "[{\"index\":0,\"ttl\":\"1 h\",\"name\":\"otherState\"}]",
                "Deduplicate(keep=[LastRow], key=[id])"));
  }

  @Test
  void givenTypeAndStateChanged_whenCheck_thenOneBreakForEachField() {
    var result =
        check(
            plan("2.3", aggregateOperator()),
            plan(
                "2.3",
                aggregateOperator()
                    .put("type", "stream-exec-group-aggregate_2")
                    .put("state", "otherState")));

    var aggregateDescription = "GroupAggregate(groupBy=[customerid])";
    assertThat(result.breaks())
        .containsExactly(
            new ChangedOperatorType(
                "2",
                "stream-exec-group-aggregate_1",
                "stream-exec-group-aggregate_2",
                aggregateDescription),
            new ChangedStateField(
                "2", "state", "\"groupAggregateState\"", "\"otherState\"", aggregateDescription));
  }

  @Test
  void givenMissingFieldAndJsonNull_whenCheck_thenNoBreaks() {
    var result = check(plan("2.3", joinOperator()), plan("2.3", joinOperator().putNull("state")));

    assertThat(result.breaks()).isEmpty();
  }

  @Test
  void givenOnlyConnectorOptionsAndConfigurationChanged_whenCheck_thenNoBreaks() {
    var oldSinkOperator = sinkOperator();
    oldSinkOperator.putObject("configuration").put("table.exec.sink.not-null-enforcer", "ERROR");
    sinkTableOptions(oldSinkOperator).put("sink.buffer-flush.max-rows", "100");
    var newSinkOperator = sinkOperator();
    newSinkOperator.putObject("configuration").put("table.exec.sink.not-null-enforcer", "DROP");
    sinkTableOptions(newSinkOperator).put("sink.buffer-flush.max-rows", "500");

    var result = check(plan("2.3", oldSinkOperator), plan("2.3", newSinkOperator));

    assertThat(result.breaks()).isEmpty();
  }

  @Test
  void givenNumericAndTextId_whenCheck_thenSameOperator() {
    var result = check(plan("2.3", sourceOperator()), plan("2.3", sourceOperator().put("id", "1")));

    assertThat(result.breaks()).isEmpty();
  }

  @Test
  void givenNewPlanWithoutNodes_whenCheck_thenEveryOldOperatorRemoved() {
    var result =
        check(
            plan("2.3", sourceOperator(), aggregateOperator(), sinkOperator()),
            OBJECT_MAPPER.createObjectNode());

    assertThat(result.breaks())
        .containsExactly(
            new RemovedOperator("1", "TableSourceScan(table=[Orders])"),
            new RemovedOperator("2", "GroupAggregate(groupBy=[customerid])"),
            new RemovedOperator("3", "Sink(table=[CustomerCount])"));
  }

  @Test
  void givenDifferentFlinkVersion_whenCheck_thenChangedFlinkVersion() {
    var result = check(plan("2.2"), plan("2.3"));

    assertThat(result.notes()).containsExactly(new ChangedFlinkVersion("2.2", "2.3"));
    assertThat(result.notes().get(0).level()).isEqualTo(Level.NOTICE);
    assertThat(result.notes().get(0).toMessage())
        .isEqualTo("The Flink version changes: 2.2 -> 2.3");
  }

  @Test
  void givenSameFlinkVersion_whenCheck_thenNoChangedFlinkVersion() {
    assertThat(check(plan("2.3"), plan("2.3")).notes()).isEmpty();
  }

  @Test
  void givenNewPlanWithoutFlinkVersion_whenCheck_thenNoChangedFlinkVersion() {
    assertThat(check(plan("2.3"), OBJECT_MAPPER.createObjectNode()).notes()).isEmpty();
  }

  @Test
  void givenOldPlanWithoutFlinkVersion_whenCheck_thenNoChangedFlinkVersion() {
    assertThat(check(OBJECT_MAPPER.createObjectNode(), plan("2.3")).notes()).isEmpty();
  }

  @Test
  void givenDifferentMaxParallelism_whenCheck_thenChangedMaxParallelism() {
    var result =
        FlinkStateUpgradeCheck.check(
            jobPlan(plan("2.3"), Optional.of(128)), jobPlan(plan("2.3"), Optional.of(144)));

    assertThat(result.breaks()).containsExactly(new ChangedMaxParallelism(128, 144));
    assertThat(result.breaks().get(0).toMessage())
        .isEqualTo("pipeline.max-parallelism differs: 128 -> 144");
    assertThat(result.notes()).isEmpty();
  }

  @Test
  void givenSameMaxParallelism_whenCheck_thenNoBreaksAndNoNotes() {
    var result =
        FlinkStateUpgradeCheck.check(
            jobPlan(plan("2.3"), Optional.of(128)), jobPlan(plan("2.3"), Optional.of(128)));

    assertThat(result.breaks()).isEmpty();
    assertThat(result.notes()).isEmpty();
  }

  @Test
  void givenOnlyOldMaxParallelism_whenCheck_thenMaxParallelismOnlyInOldBuild() {
    var result =
        FlinkStateUpgradeCheck.check(
            jobPlan(plan("2.3"), Optional.of(128)), jobPlan(plan("2.3"), Optional.empty()));

    assertThat(result.breaks()).isEmpty();
    assertThat(result.notes()).containsExactly(new MaxParallelismOnlyInOldBuild(128));
    assertThat(result.notes().get(0).level()).isEqualTo(Level.WARN);
    assertThat(result.notes().get(0).toMessage())
        .startsWith("Only the old build sets pipeline.max-parallelism (128).")
        .contains("Flink takes this value from the savepoint.");
  }

  @Test
  void givenOnlyNewMaxParallelism_whenCheck_thenMaxParallelismOnlyInNewBuild() {
    var result =
        FlinkStateUpgradeCheck.check(
            jobPlan(plan("2.3"), Optional.empty()), jobPlan(plan("2.3"), Optional.of(144)));

    assertThat(result.breaks()).isEmpty();
    assertThat(result.notes()).containsExactly(new MaxParallelismOnlyInNewBuild(144));
    assertThat(result.notes().get(0).level()).isEqualTo(Level.WARN);
    assertThat(result.notes().get(0).toMessage())
        .startsWith("Only the new build sets pipeline.max-parallelism (144).");
  }

  /** Checks two plans that do not set {@code pipeline.max-parallelism}. */
  private static StateUpgradeResult check(JsonNode oldCompiledPlan, JsonNode newCompiledPlan) {
    return FlinkStateUpgradeCheck.check(
        jobPlan(oldCompiledPlan, Optional.empty()), jobPlan(newCompiledPlan, Optional.empty()));
  }

  private static FlinkJobPlan jobPlan(JsonNode compiledPlan, Optional<Integer> maxParallelism) {
    return new FlinkJobPlan(compiledPlan, maxParallelism);
  }

  private static ObjectNode sourceOperator() {
    return operator(1, "stream-exec-table-source-scan_2", "TableSourceScan(table=[Orders])");
  }

  private static ObjectNode aggregateOperator() {
    return operator(2, "stream-exec-group-aggregate_1", "GroupAggregate(groupBy=[customerid])")
        .put("state", "groupAggregateState");
  }

  private static ObjectNode sinkOperator() {
    return operator(3, "stream-exec-sink_1", "Sink(table=[CustomerCount])")
        .put("inputChangelogMode", "INSERT, UPDATE_AFTER");
  }

  /** Returns the table options of a sink operator, in the place where Flink writes them. */
  private static ObjectNode sinkTableOptions(ObjectNode sinkOperator) {
    return sinkOperator
        .putObject("dynamicTableSink")
        .putObject("table")
        .putObject("resolvedTable")
        .putObject("options")
        .put("connector", "jdbc");
  }

  /** A deduplicate operator with one state entry, in the form that Flink writes it. */
  private static ObjectNode deduplicateOperator(String ttl) {
    var deduplicateOperator =
        operator(5, "stream-exec-deduplicate_1", "Deduplicate(keep=[LastRow], key=[id])");
    deduplicateOperator
        .putArray("state")
        .addObject()
        .put("index", 0)
        .put("ttl", ttl)
        .put("name", "deduplicateState");
    return deduplicateOperator;
  }

  /**
   * Sets the {@code outputType} of the operator to a {@code ROW} with the columns, in the form that
   * Flink writes it. Each column is {@code "<name> <type>"}, for example {@code "id INT"}.
   */
  private static ObjectNode withOutputType(ObjectNode operator, String... columns) {
    var columnFields = operator.putObject("outputType").put("type", "ROW").putArray("fields");
    for (var column : columns) {
      var nameAndType = column.split(" ", 2);
      columnFields.addObject().put("name", nameAndType[0]).put("fieldType", nameAndType[1]);
    }
    return operator;
  }

  private static ObjectNode joinOperator() {
    return operator(4, "stream-exec-join_1", "Join(joinType=[InnerJoin])");
  }

  private static ObjectNode operator(int id, String type, String description) {
    return OBJECT_MAPPER
        .createObjectNode()
        .put("id", id)
        .put("type", type)
        .put("description", description);
  }

  private static JsonNode plan(String flinkVersion, ObjectNode... operators) {
    var plan = OBJECT_MAPPER.createObjectNode().put("flinkVersion", flinkVersion);
    plan.putArray("nodes").addAll(List.of(operators));
    return plan;
  }
}
