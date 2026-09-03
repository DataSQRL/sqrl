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
package com.datasqrl.planner;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.planner.analyzer.TableAnalysis;
import java.util.List;
import java.util.Set;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.junit.jupiter.api.Test;

class TableAnalysisLookupTest {

  @Test
  void givenAliasHints_whenNormalizeRelnode_thenIgnoresAliasesWithoutMutatingPlan() {
    var typeFactory = new JavaTypeFactoryImpl();
    var cluster = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(typeFactory));
    var values = LogicalValues.createOneRow(cluster);
    var hintedValues = values.withHints(List.of(RelHint.builder("ALIAS").build()));
    var inputRef = hintedValues.getCluster().getRexBuilder().makeInputRef(hintedValues, 0);
    var hintedProject =
        LogicalProject.create(
            hintedValues,
            List.of(RelHint.builder("ALIAS").build()),
            List.of(inputRef),
            List.of("id"));
    var unhintedInputRef = values.getCluster().getRexBuilder().makeInputRef(values, 0);
    var unhintedProject =
        LogicalProject.create(values, List.of(), List.of(unhintedInputRef), List.of("id"));

    var lookup = new TableAnalysisLookup();
    var normalized = lookup.normalizeRelnode(hintedProject);

    assertThat(hintedProject.getHints()).hasSize(1);
    assertThat(((Hintable) hintedValues).getHints()).hasSize(1);
    assertThat(((Hintable) normalized).getHints()).isEmpty();
    assertThat(((Hintable) normalized.getInput(0)).getHints()).isEmpty();
    assertThat(normalized.deepEquals(lookup.normalizeRelnode(unhintedProject))).isTrue();
  }

  @Test
  void givenExecutionHints_whenNormalizeRelnode_thenPreservesHintsAsViewIdentity() {
    var typeFactory = new JavaTypeFactoryImpl();
    var cluster = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(typeFactory));
    var values = LogicalValues.createOneRow(cluster);
    var inputRef = values.getCluster().getRexBuilder().makeInputRef(values, 0);
    var ttlProject =
        LogicalProject.create(
            values,
            List.of(RelHint.builder("STATE_TTL").build()),
            List.of(inputRef),
            List.of("id"));
    var unhintedProject =
        LogicalProject.create(values, List.of(), List.of(inputRef), List.of("id"));

    var lookup = new TableAnalysisLookup();
    var normalized = lookup.normalizeRelnode(ttlProject);

    assertThat(((Hintable) normalized).getHints())
        .containsExactly(RelHint.builder("STATE_TTL").build());
    assertThat(normalized.deepEquals(lookup.normalizeRelnode(unhintedProject))).isFalse();
  }

  @Test
  void givenIdenticalViews_whenLookupViewWithReferencedView_thenSelectsReferencedView() {
    var typeFactory = new JavaTypeFactoryImpl();
    var cluster = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(typeFactory));
    var relNode = LogicalValues.createOneRow(cluster);
    var lookup = new TableAnalysisLookup();
    var explicitIndexes = view(lookup, "ExplicitIndexes", relNode);
    var noIndexes = view(lookup, "NoIndexes", relNode);
    lookup.registerTable(explicitIndexes);
    lookup.registerTable(noIndexes);

    assertThat(lookup.lookupView(relNode, Set.of(explicitIndexes.getObjectIdentifier())))
        .get()
        .isEqualTo(explicitIndexes);
  }

  private static TableAnalysis view(
      TableAnalysisLookup lookup, String name, LogicalValues relNode) {
    return TableAnalysis.builder()
        .objectIdentifier(ObjectIdentifier.of("catalog", "database", name))
        .originalRelnode(lookup.normalizeRelnode(relNode))
        .collapsedRelnode(relNode)
        .build();
  }
}
