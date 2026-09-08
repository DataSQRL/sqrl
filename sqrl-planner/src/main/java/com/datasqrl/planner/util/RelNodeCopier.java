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
package com.datasqrl.planner.util;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableSet;

/**
 * Deep-copies a relational tree with fresh correlation ids, so that every reference to a planned
 * view hands Flink a distinct tree just like expanding the view's SQL text would. Flink's optimizer
 * treats RelNode instances shared between queries as common sub-graphs, and duplicate correlation
 * ids within one query break decorrelation. The ids are shifted by a constant so that {@link
 * com.datasqrl.planner.TableAnalysisLookup} still recognizes the copy as the same view.
 */
public class RelNodeCopier extends RelShuttleImpl {

  private final RelOptCluster cluster;
  private final int correlationIdShift;
  private final RexShuttle rexCopier =
      new RexShuttle() {
        @Override
        public RexNode visitCorrelVariable(RexCorrelVariable variable) {
          return cluster.getRexBuilder().makeCorrel(variable.getType(), shift(variable.id));
        }

        @Override
        public RexNode visitSubQuery(RexSubQuery subQuery) {
          var copied = (RexSubQuery) super.visitSubQuery(subQuery);
          return copied.clone(subQuery.rel.accept(RelNodeCopier.this));
        }
      };

  public static RelNode copy(RelNode relNode) {
    return relNode.accept(new RelNodeCopier(relNode));
  }

  private RelNodeCopier(RelNode relNode) {
    this.cluster = relNode.getCluster();
    this.correlationIdShift = reserveCorrelationIds(relNode);
  }

  private int reserveCorrelationIds(RelNode relNode) {
    var collector = new CorrelationIdCollector();
    relNode.accept(collector);
    if (collector.ids.isEmpty()) {
      return 0;
    }
    int min = Collections.min(collector.ids);
    int max = Collections.max(collector.ids);
    var first = cluster.createCorrel().getId();
    for (var i = min + 1; i <= max; i++) {
      cluster.createCorrel();
    }
    return first - min;
  }

  private CorrelationId shift(CorrelationId id) {
    return new CorrelationId(id.getId() + correlationIdShift);
  }

  private ImmutableSet<CorrelationId> shift(Set<CorrelationId> ids) {
    return ids.stream().map(this::shift).collect(ImmutableSet.toImmutableSet());
  }

  @Override
  protected RelNode visitChild(RelNode parent, int i, RelNode child) {
    return visitChildren(parent);
  }

  @Override
  protected RelNode visitChildren(RelNode rel) {
    var inputs = rel.getInputs().stream().map(input -> input.accept(this)).toList();
    return copy(rel, inputs).accept(rexCopier);
  }

  @Override
  public RelNode visit(TableScan scan) {
    if (scan instanceof LogicalTableScan logicalScan) {
      return new LogicalTableScan(
          cluster, scan.getTraitSet(), logicalScan.getHints(), scan.getTable());
    }
    return scan;
  }

  @Override
  public RelNode visit(LogicalValues values) {
    return visitChildren(values);
  }

  private RelNode copy(RelNode rel, List<RelNode> inputs) {
    var traitSet = rel.getTraitSet();
    if (rel instanceof LogicalCorrelate correlate) {
      return correlate.copy(
          traitSet,
          inputs.get(0),
          inputs.get(1),
          shift(correlate.getCorrelationId()),
          correlate.getRequiredColumns(),
          correlate.getJoinType());
    } else if (rel instanceof LogicalFilter filter) {
      return new LogicalFilter(
          cluster,
          traitSet,
          filter.getHints(),
          inputs.get(0),
          filter.getCondition(),
          shift(filter.getVariablesSet()));
    } else if (rel instanceof LogicalProject project) {
      return new LogicalProject(
          cluster,
          traitSet,
          project.getHints(),
          inputs.get(0),
          project.getProjects(),
          project.getRowType(),
          shift(project.getVariablesSet()));
    } else if (rel instanceof LogicalJoin join) {
      return new LogicalJoin(
          cluster,
          traitSet,
          join.getHints(),
          inputs.get(0),
          inputs.get(1),
          join.getCondition(),
          shift(join.getVariablesSet()),
          join.getJoinType(),
          join.isSemiJoinDone(),
          ImmutableList.copyOf(join.getSystemFieldList()));
    }
    return rel.copy(traitSet, inputs);
  }

  private static class CorrelationIdCollector extends RelShuttleImpl {

    private final Set<Integer> ids = new HashSet<>();
    private final RexShuttle rexCollector =
        new RexShuttle() {
          @Override
          public RexNode visitCorrelVariable(RexCorrelVariable variable) {
            ids.add(variable.id.getId());
            return variable;
          }

          @Override
          public RexNode visitSubQuery(RexSubQuery subQuery) {
            subQuery.rel.accept(CorrelationIdCollector.this);
            return super.visitSubQuery(subQuery);
          }
        };

    private void collect(RelNode rel) {
      rel.getVariablesSet().forEach(id -> ids.add(id.getId()));
      rel.accept(rexCollector);
    }

    @Override
    protected RelNode visitChild(RelNode parent, int i, RelNode child) {
      return visitChildren(parent);
    }

    @Override
    protected RelNode visitChildren(RelNode rel) {
      collect(rel);
      rel.getInputs().forEach(input -> input.accept(this));
      return rel;
    }
  }
}
