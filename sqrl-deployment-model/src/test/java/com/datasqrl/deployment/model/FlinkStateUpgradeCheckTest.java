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

import com.datasqrl.deployment.model.FlinkStateUpgradeCheck.MaxParallelismResult;
import com.datasqrl.deployment.model.StateUpgradeBreak.ChangedOperatorType;
import com.datasqrl.deployment.model.StateUpgradeBreak.ChangedStateField;
import com.datasqrl.deployment.model.StateUpgradeBreak.RemovedOperator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkStateUpgradeCheckTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  void givenSamePlan_whenFindBreaks_thenEmpty() {
    var oldPlan = plan("2.3", sourceOperator(), aggregateOperator(), sinkOperator());
    var newPlan = plan("2.3", sourceOperator(), aggregateOperator(), sinkOperator());

    assertThat(FlinkStateUpgradeCheck.findBreaks(oldPlan, newPlan)).isEmpty();
  }

  @Test
  void givenOperatorOnlyInNewPlan_whenFindBreaks_thenEmpty() {
    var oldPlan = plan("2.3", sourceOperator(), aggregateOperator());
    var newPlan = plan("2.3", sourceOperator(), aggregateOperator(), sinkOperator());

    assertThat(FlinkStateUpgradeCheck.findBreaks(oldPlan, newPlan)).isEmpty();
  }

  @Test
  void givenOperatorMissingInNewPlan_whenFindBreaks_thenRemovedOperator() {
    var oldPlan = plan("2.3", sourceOperator(), aggregateOperator(), sinkOperator());
    var newPlan = plan("2.3", sourceOperator(), sinkOperator());

    var stateUpgradeBreaks = FlinkStateUpgradeCheck.findBreaks(oldPlan, newPlan);

    assertThat(stateUpgradeBreaks)
        .containsExactly(new RemovedOperator("2", "GroupAggregate(groupBy=[customerid])"));
    assertThat(stateUpgradeBreaks.get(0).toMessage())
        .isEqualTo("operator 2 removed: GroupAggregate(groupBy=[customerid])");
  }

  @Test
  void givenTypeChanged_whenFindBreaks_thenChangedOperatorType() {
    var oldPlan = plan("2.3", sinkOperator());
    var newPlan = plan("2.3", sinkOperator().put("type", "stream-exec-sink_2"));

    var stateUpgradeBreaks = FlinkStateUpgradeCheck.findBreaks(oldPlan, newPlan);

    assertThat(stateUpgradeBreaks)
        .containsExactly(
            new ChangedOperatorType(
                "3", "stream-exec-sink_1", "stream-exec-sink_2", "Sink(table=[CustomerCount])"));
    assertThat(stateUpgradeBreaks.get(0).toMessage())
        .isEqualTo(
            "operator 3 changed: field 'type' differs:"
                + " stream-exec-sink_1 -> stream-exec-sink_2 (Sink(table=[CustomerCount]))");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"state", "stateMetadata", "joinSpec", "inputProperties", "inputChangelogMode"})
  void givenStateFieldChanged_whenFindBreaks_thenChangedStateFieldOnThatField(String fieldName) {
    var oldPlan = plan("2.3", joinOperator());
    var newPlan = plan("2.3", joinOperator().put(fieldName, "changed"));

    assertThat(FlinkStateUpgradeCheck.findBreaks(oldPlan, newPlan))
        .containsExactly(
            new ChangedStateField(
                "4", fieldName, "Join(joinType=[InnerJoin])", "Join(joinType=[InnerJoin])"));
  }

  @Test
  void givenStateAndDescriptionChanged_whenFindBreaks_thenMessageShowsBothDescriptions() {
    var oldPlan = plan("2.3", aggregateOperator());
    var newPlan =
        plan(
            "2.3",
            aggregateOperator()
                .put("state", "otherState")
                .put("description", "GroupAggregate(groupBy=[customerid, region])"));

    var stateUpgradeBreaks = FlinkStateUpgradeCheck.findBreaks(oldPlan, newPlan);

    assertThat(stateUpgradeBreaks).hasSize(1);
    assertThat(stateUpgradeBreaks.get(0).toMessage())
        .isEqualTo(
            "operator 2 changed: field 'state' differs"
                + " (old: GroupAggregate(groupBy=[customerid]),"
                + " new: GroupAggregate(groupBy=[customerid, region]))");
  }

  @Test
  void givenTypeAndStateChanged_whenFindBreaks_thenOneBreakForEachField() {
    var oldPlan = plan("2.3", aggregateOperator());
    var newPlan =
        plan(
            "2.3",
            aggregateOperator()
                .put("type", "stream-exec-group-aggregate_2")
                .put("state", "otherState"));

    var aggregateDescription = "GroupAggregate(groupBy=[customerid])";
    assertThat(FlinkStateUpgradeCheck.findBreaks(oldPlan, newPlan))
        .containsExactly(
            new ChangedOperatorType(
                "2",
                "stream-exec-group-aggregate_1",
                "stream-exec-group-aggregate_2",
                aggregateDescription),
            new ChangedStateField("2", "state", aggregateDescription, aggregateDescription));
  }

  @Test
  void givenMissingFieldAndJsonNull_whenFindBreaks_thenEmpty() {
    var oldPlan = plan("2.3", joinOperator());
    var newPlan = plan("2.3", joinOperator().putNull("state"));

    assertThat(FlinkStateUpgradeCheck.findBreaks(oldPlan, newPlan)).isEmpty();
  }

  @Test
  void givenNumericAndTextId_whenFindBreaks_thenSameOperator() {
    var oldPlan = plan("2.3", sourceOperator());
    var newPlan = plan("2.3", sourceOperator().put("id", "1"));

    assertThat(FlinkStateUpgradeCheck.findBreaks(oldPlan, newPlan)).isEmpty();
  }

  @Test
  void givenPlanWithoutNodes_whenFindBreaks_thenEveryOldOperatorRemoved() {
    var oldPlan = plan("2.3", sourceOperator(), aggregateOperator(), sinkOperator());
    var newPlan = OBJECT_MAPPER.createObjectNode();

    assertThat(FlinkStateUpgradeCheck.findBreaks(oldPlan, newPlan))
        .allMatch(RemovedOperator.class::isInstance)
        .extracting(StateUpgradeBreak::operatorId)
        .containsExactly("1", "2", "3");
  }

  @Test
  void givenDifferentFlinkVersion_whenFlinkVersionChange_thenOldArrowNew() {
    assertThat(FlinkStateUpgradeCheck.flinkVersionChange(plan("2.2"), plan("2.3")))
        .contains("2.2 -> 2.3");
  }

  @Test
  void givenSameFlinkVersion_whenFlinkVersionChange_thenEmpty() {
    assertThat(FlinkStateUpgradeCheck.flinkVersionChange(plan("2.3"), plan("2.3"))).isEmpty();
  }

  @Test
  void givenPlanWithoutFlinkVersion_whenFlinkVersionChange_thenEmpty() {
    assertThat(
            FlinkStateUpgradeCheck.flinkVersionChange(
                plan("2.3"), OBJECT_MAPPER.createObjectNode()))
        .isEmpty();
  }

  static Stream<Arguments> maxParallelismCases() {
    return Stream.of(
        Arguments.of(Optional.of(128), Optional.of(128), MaxParallelismResult.SAME),
        Arguments.of(Optional.of(128), Optional.of(144), MaxParallelismResult.DIFFERENT),
        Arguments.of(Optional.of(128), Optional.empty(), MaxParallelismResult.ONLY_OLD_SET),
        Arguments.of(Optional.empty(), Optional.of(128), MaxParallelismResult.ONLY_NEW_SET),
        Arguments.of(Optional.empty(), Optional.empty(), MaxParallelismResult.NOT_SET));
  }

  @ParameterizedTest
  @MethodSource("maxParallelismCases")
  void givenMaxParallelismValues_whenCompareMaxParallelism_thenExpectedResult(
      Optional<Integer> oldValue, Optional<Integer> newValue, MaxParallelismResult expectedResult) {
    assertThat(FlinkStateUpgradeCheck.compareMaxParallelism(oldValue, newValue))
        .isEqualTo(expectedResult);
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
