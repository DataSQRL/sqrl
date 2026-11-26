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
import com.datasqrl.deployment.model.StateUpgradeBreak.OldState;
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
        .containsExactly(
            new RemovedOperator("2", "GroupAggregate(groupBy=[customerid])", OldState.UNCLAIMED));
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
                "3",
                "stream-exec-sink_1",
                "stream-exec-sink_2",
                "Sink(table=[CustomerCount])",
                OldState.NONE));
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
                "4",
                fieldName,
                "missing",
                "\"changed\"",
                "Join(joinType=[InnerJoin])",
                OldState.INCOMPATIBLE));
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
                "Deduplicate(keep=[LastRow], key=[id])",
                OldState.INCOMPATIBLE));
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
                "Deduplicate(keep=[LastRow], key=[id])",
                OldState.INCOMPATIBLE));
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
  void givenRenamedColumnsInTextOutputType_whenCheck_thenNoBreaks() {
    var oldAggregateOperator =
        aggregateOperator().put("outputType", "ROW<`id` INT, `cnt` BIGINT NOT NULL>");
    var newAggregateOperator =
        aggregateOperator().put("outputType", "ROW<`customer` INT, `total` BIGINT NOT NULL>");

    var result = check(plan("2.3", oldAggregateOperator), plan("2.3", newAggregateOperator));

    assertThat(result.breaks()).isEmpty();
  }

  @Test
  void givenNewColumnInTextOutputType_whenCheck_thenChangedOutputTypeShowsText() {
    var oldAggregateOperator =
        aggregateOperator().put("outputType", "ROW<`id` INT, `cnt` BIGINT NOT NULL>");
    var newAggregateOperator =
        aggregateOperator()
            .put("outputType", "ROW<`id` INT, `cnt` BIGINT NOT NULL, `total` BIGINT>");

    var result = check(plan("2.3", oldAggregateOperator), plan("2.3", newAggregateOperator));

    assertThat(result.breaks())
        .containsExactly(
            new ChangedStateField(
                "2",
                "outputType",
                "ROW<`id` INT, `cnt` BIGINT NOT NULL>",
                "ROW<`id` INT, `cnt` BIGINT NOT NULL, `total` BIGINT>",
                "GroupAggregate(groupBy=[customerid])",
                OldState.INCOMPATIBLE));
  }

  @Test
  void givenRenamedNestedFieldInTextOutputType_whenCheck_thenChangedOutputType() {
    var oldDeduplicateOperator =
        deduplicateOperator("0 ms").put("outputType", "ROW<`id` INT, `payload` ROW<`a` INT>>");
    var newDeduplicateOperator =
        deduplicateOperator("0 ms").put("outputType", "ROW<`id` INT, `payload` ROW<`b` INT>>");

    var result = check(plan("2.3", oldDeduplicateOperator), plan("2.3", newDeduplicateOperator));

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
  void givenNewColumnOnGlobalWindowAggregate_whenCheck_thenChangedOutputType() {
    var result =
        check(
            plan("2.3", withOutputType(globalWindowAggregateOperator(), "cnt BIGINT")),
            plan(
                "2.3",
                withOutputType(globalWindowAggregateOperator(), "cnt BIGINT", "max_val BIGINT")));

    assertThat(result.breaks())
        .containsExactly(
            new ChangedStateField(
                "6",
                "outputType",
                "[cnt BIGINT]",
                "[cnt BIGINT, max_val BIGINT]",
                "GlobalWindowAggregate(window=[TUMBLE(size=[1 min])])",
                OldState.INCOMPATIBLE));
  }

  @Test
  void givenNewColumnOnIntervalJoin_whenCheck_thenChangedOutputType() {
    var result =
        check(
            plan("2.3", withOutputType(intervalJoinOperator(), "id INT", "order_id INT")),
            plan(
                "2.3",
                withOutputType(intervalJoinOperator(), "id INT", "order_id INT", "amount INT")));

    assertThat(result.breaks())
        .singleElement()
        .extracting(StateUpgradeBreak::oldState)
        .isEqualTo(OldState.INCOMPATIBLE);
  }

  @Test
  void givenGlobalWindowAggregateChangesKind_whenCheck_thenUnclaimedState() {
    var newOperatorAtSameId =
        operator(6, "stream-exec-local-window-aggregate_1", "LocalWindowAggregate()");

    var result =
        check(plan("2.3", globalWindowAggregateOperator()), plan("2.3", newOperatorAtSameId));

    assertThat(result.breaks())
        .singleElement()
        .extracting(StateUpgradeBreak::oldState)
        .isEqualTo(OldState.UNCLAIMED);
  }

  @Test
  void givenNewColumnOnUnknownOperatorKind_whenCheck_thenChangedOutputType() {
    var unknownOperator = operator(7, "stream-exec-future-operator_1", "FutureOperator()");

    var result =
        check(
            plan("2.3", withOutputType(unknownOperator.deepCopy(), "id INT")),
            plan("2.3", withOutputType(unknownOperator.deepCopy(), "id INT", "extra INT")));

    assertThat(result.breaks())
        .singleElement()
        .extracting(StateUpgradeBreak::oldState)
        .isEqualTo(OldState.INCOMPATIBLE);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "stream-exec-calc_1",
        "stream-exec-exchange_1",
        "stream-exec-local-window-aggregate_1",
        "stream-exec-sink_1"
      })
  void givenNewColumnOnOperatorKindWithoutState_whenCheck_thenNoBreaks(String operatorType) {
    var result =
        check(
            plan("2.3", withOutputType(operator(8, operatorType, "Operator()"), "id INT")),
            plan(
                "2.3",
                withOutputType(operator(8, operatorType, "Operator()"), "id INT", "extra INT")));

    assertThat(result.breaks()).isEmpty();
  }

  @Test
  void givenChangedAggregateFunction_whenCheck_thenChangedAggCalls() {
    var result =
        check(
            plan("2.3", withAggCall(aggregateOperator(), aggCall("total", "$SUM$1", 1))),
            plan("2.3", withAggCall(aggregateOperator(), aggCall("total", "$MAX$1", 1))));

    assertThat(result.breaks())
        .containsExactly(
            new ChangedStateField(
                "2",
                "aggCalls",
                "[SUM]",
                "[MAX]",
                "GroupAggregate(groupBy=[customerid])",
                OldState.INCOMPATIBLE));
  }

  @Test
  void givenRenamedAggregateColumnAndMovedArgument_whenCheck_thenNoBreaks() {
    var result =
        check(
            plan("2.3", withAggCall(aggregateOperator(), aggCall("total", "$SUM$1", 1))),
            plan("2.3", withAggCall(aggregateOperator(), aggCall("value_sum", "$SUM$1", 2))));

    assertThat(result.breaks()).isEmpty();
  }

  @Test
  void givenSumChangedToSum0_whenCheck_thenNoBreaks() {
    var result =
        check(
            plan("2.3", withAggCall(aggregateOperator(), aggCall("total", "$SUM$1", 1))),
            plan("2.3", withAggCall(aggregateOperator(), aggCall("total", "$$SUM0$1", 1))));

    assertThat(result.breaks()).isEmpty();
  }

  @Test
  void givenDistinctAddedToAggregateFunction_whenCheck_thenChangedAggCalls() {
    var distinctCount = aggCall("cnt", "$COUNT$1", 1).put("distinct", true);

    var result =
        check(
            plan("2.3", withAggCall(aggregateOperator(), aggCall("cnt", "$COUNT$1", 1))),
            plan("2.3", withAggCall(aggregateOperator(), distinctCount)));

    assertThat(result.breaks())
        .singleElement()
        .extracting(stateUpgradeBreak -> stateUpgradeBreak.toMessage())
        .asString()
        .contains("[COUNT] -> [COUNT DISTINCT]");
  }

  @Test
  void givenChangedFunctionInOverAggregate_whenCheck_thenChangedAggCalls() {
    var result =
        check(
            plan("2.3", overAggregateOperator(aggCall("total", "$SUM$1", 1))),
            plan("2.3", overAggregateOperator(aggCall("total", "$MIN$1", 1))));

    assertThat(result.breaks())
        .singleElement()
        .extracting(stateUpgradeBreak -> stateUpgradeBreak.toMessage())
        .asString()
        .contains("field 'aggCalls' differs: [SUM] -> [MIN]");
  }

  @Test
  void givenNewColumnOnSinkWithMaterializeState_whenCheck_thenChangedOutputType() {
    var result =
        check(
            plan("2.3", withOutputType(materializedSinkOperator(), "id INT", "total BIGINT")),
            plan(
                "2.3",
                withOutputType(
                    materializedSinkOperator(), "id INT", "total BIGINT", "extra BIGINT")));

    assertThat(result.breaks())
        .containsExactly(
            new ChangedStateField(
                "3",
                "outputType",
                "[id INT, total BIGINT]",
                "[id INT, total BIGINT, extra BIGINT]",
                "Sink(table=[CustomerCount], upsertMaterialize=[true])",
                OldState.INCOMPATIBLE));
  }

  @Test
  void givenSinkWithoutStateChangesKind_whenCheck_thenUnclaimedState() {
    var result =
        check(
            plan("2.3", sinkOperator()),
            plan("2.3", operator(3, "stream-exec-calc_1", "Calc(select=[customerid])")));

    assertThat(result.breaks())
        .filteredOn(ChangedOperatorType.class::isInstance)
        .singleElement()
        .extracting(StateUpgradeBreak::oldState)
        .isEqualTo(OldState.UNCLAIMED);
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
                "Deduplicate(keep=[LastRow], key=[id])",
                OldState.UNCLAIMED));
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
                aggregateDescription,
                OldState.INCOMPATIBLE),
            new ChangedStateField(
                "2",
                "state",
                "\"groupAggregateState\"",
                "\"otherState\"",
                aggregateDescription,
                OldState.INCOMPATIBLE));
  }

  @Test
  void givenOperatorWithStateChangesKind_whenCheck_thenUnclaimedState() {
    var newOperatorAtSameId = operator(2, "stream-exec-calc_1", "Calc(select=[customerid])");

    var result = check(plan("2.3", aggregateOperator()), plan("2.3", newOperatorAtSameId));

    assertThat(result.breaks())
        .filteredOn(ChangedOperatorType.class::isInstance)
        .singleElement()
        .extracting(StateUpgradeBreak::oldState)
        .isEqualTo(OldState.UNCLAIMED);
  }

  @Test
  void givenSourceChangesKind_whenCheck_thenUnclaimedState() {
    var result =
        check(
            plan("2.3", sourceOperator()),
            plan(
                "2.3", operator(1, "stream-exec-exchange_1", "Exchange(distribution=[hash[id]])")));

    assertThat(result.breaks())
        .singleElement()
        .extracting(StateUpgradeBreak::oldState)
        .isEqualTo(OldState.UNCLAIMED);
  }

  @Test
  void givenOperatorWithoutStateChangesKind_whenCheck_thenNoState() {
    var result =
        check(
            plan("2.3", operator(2, "stream-exec-calc_1", "Calc(select=[id])")),
            plan(
                "2.3", operator(2, "stream-exec-exchange_1", "Exchange(distribution=[hash[id]])")));

    assertThat(result.breaks())
        .singleElement()
        .extracting(StateUpgradeBreak::oldState)
        .isEqualTo(OldState.NONE);
  }

  @Test
  void givenRemovedStateEntry_whenCheck_thenUnclaimedState() {
    var oldSinkOperator = sinkOperator();
    oldSinkOperator
        .putArray("state")
        .addObject()
        .put("index", 0)
        .put("ttl", "0 ms")
        .put("name", "sinkMaterializeState");

    var result = check(plan("2.3", oldSinkOperator), plan("2.3", sinkOperator()));

    assertThat(result.breaks())
        .singleElement()
        .extracting(StateUpgradeBreak::oldState)
        .isEqualTo(OldState.UNCLAIMED);
  }

  @Test
  void givenNewStateEntry_whenCheck_thenNoState() {
    var newSinkOperator = sinkOperator();
    newSinkOperator
        .putArray("state")
        .addObject()
        .put("index", 0)
        .put("ttl", "0 ms")
        .put("name", "sinkMaterializeState");

    var result = check(plan("2.3", sinkOperator()), plan("2.3", newSinkOperator));

    assertThat(result.breaks())
        .singleElement()
        .extracting(StateUpgradeBreak::oldState)
        .isEqualTo(OldState.NONE);
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
            new RemovedOperator("1", "TableSourceScan(table=[Orders])", OldState.UNCLAIMED),
            new RemovedOperator("2", "GroupAggregate(groupBy=[customerid])", OldState.UNCLAIMED),
            new RemovedOperator("3", "Sink(table=[CustomerCount])", OldState.UNCLAIMED));
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

  /** Sets the {@code aggCalls} of the operator to the one aggregate call. */
  private static ObjectNode withAggCall(ObjectNode operator, ObjectNode aggCall) {
    operator.putArray("aggCalls").add(aggCall);
    return operator;
  }

  /** An aggregate call in the form that Flink writes it, for example {@code "$SUM$1"}. */
  private static ObjectNode aggCall(String name, String internalName, int argumentIndex) {
    var aggCall =
        OBJECT_MAPPER
            .createObjectNode()
            .put("name", name)
            .put("internalName", internalName)
            .put("filterArg", -1)
            .put("distinct", false)
            .put("approximate", false)
            .put("ignoreNulls", false)
            .put("type", "BIGINT NOT NULL");
    aggCall.putArray("argList").add(argumentIndex);
    return aggCall;
  }

  /** An over aggregate, which keeps its aggregate calls in {@code overSpec.groups[].aggCalls}. */
  private static ObjectNode overAggregateOperator(ObjectNode aggCall) {
    var overAggregateOperator =
        operator(10, "stream-exec-over-aggregate_1", "OverAggregate(orderBy=[ts ASC])");
    overAggregateOperator
        .putObject("overSpec")
        .putArray("groups")
        .addObject()
        .putArray("aggCalls")
        .add(aggCall);
    return overAggregateOperator;
  }

  /**
   * A sink with an upsert materializer, in the form that Flink writes it. The materializer stores
   * the rows of each primary key, so the sink has a {@code state} entry.
   */
  private static ObjectNode materializedSinkOperator() {
    var materializedSinkOperator =
        operator(3, "stream-exec-sink_2", "Sink(table=[CustomerCount], upsertMaterialize=[true])");
    materializedSinkOperator
        .putArray("state")
        .addObject()
        .put("index", 0)
        .put("ttl", "0 ms")
        .put("name", "sinkMaterializeState");
    return materializedSinkOperator;
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

  /**
   * A window aggregate in the form that Flink writes it: it keeps state, but has no state field.
   */
  private static ObjectNode globalWindowAggregateOperator() {
    return operator(
        6,
        "stream-exec-global-window-aggregate_1",
        "GlobalWindowAggregate(window=[TUMBLE(size=[1 min])])");
  }

  /** An interval join in the form that Flink writes it: it keeps state, but has no state field. */
  private static ObjectNode intervalJoinOperator() {
    var intervalJoinOperator =
        operator(9, "stream-exec-interval-join_1", "IntervalJoin(joinType=[InnerJoin])");
    intervalJoinOperator.putObject("intervalJoinSpec").put("joinType", "INNER");
    return intervalJoinOperator;
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
