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
package com.datasqrl.engine.database.relational;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;

@RequiredArgsConstructor
class DuckDbMaterializedScanCtePlanner {

  private static final String CTE_PREFIX = "__sqrl_iceberg_scan_";
  private static final double LOG_2 = Math.log(2);

  private final int cardinalityDivisor;

  List<MaterializedScanCte> getMaterializedScanCtes(RelNode relNode) {
    var collector = new TableScanCollector();
    var scansByTableId = collector.collect(relNode);

    var existingNames = new HashSet<>(scansByTableId.keySet());

    var ctes = new ArrayList<MaterializedScanCte>();
    var cteNumber = 0;

    for (var entry : scansByTableId.entrySet()) {
      var tableId = entry.getKey();
      var scans = entry.getValue();
      if (!shouldMaterialize(scans)) {
        continue;
      }

      var cteName = CTE_PREFIX + cteNumber++;
      while (!existingNames.add(cteName)) {
        cteName = CTE_PREFIX + cteNumber++;
      }

      var relNodeWithCte =
          withSharedPredicates(scans.getFirst(), collector.sharedPredicates(scans));
      ctes.add(new MaterializedScanCte(tableId, cteName, relNodeWithCte));
    }

    return ctes;
  }

  boolean shouldMaterialize(Deque<TableScan> scans) {
    if (scans.size() < 2) {
      return false;
    }

    var scan = scans.getFirst();
    var cardinality = scan.getCluster().getMetadataQuery().getRowCount(scan);
    if (cardinality == null || !Double.isFinite(cardinality) || cardinality <= 0) {
      return false;
    }

    var scanThreshold = Math.max((Math.log(cardinality) / LOG_2 - 20) / cardinalityDivisor, 1.0);

    return scans.size() > scanThreshold;
  }

  private RelNode withSharedPredicates(TableScan scan, List<RexNode> predicates) {
    if (predicates.isEmpty()) {
      return scan;
    }

    return LogicalFilter.create(
        scan, RexUtil.composeConjunction(scan.getCluster().getRexBuilder(), predicates));
  }

  static String getTableId(TableScan scan) {
    var names = scan.getTable().getQualifiedName();
    return names.get(names.size() - 1);
  }

  record MaterializedScanCte(String tableId, String name, RelNode source) {}

  private static class TableScanCollector extends RelShuttleImpl {

    private final Map<String, Deque<TableScan>> scansByTableId = new TreeMap<>();
    private final Map<TableScan, List<RexNode>> directFilterPredicates = new IdentityHashMap<>();
    private final RexShuttle subQueryRexShuttle =
        new RexShuttle() {
          @Override
          public RexNode visitSubQuery(RexSubQuery subQuery) {
            subQuery.rel.accept(TableScanCollector.this);
            return subQuery;
          }
        };

    Map<String, Deque<TableScan>> collect(RelNode relNode) {
      relNode.accept(this);
      return scansByTableId;
    }

    List<RexNode> sharedPredicates(Deque<TableScan> scans) {
      Set<RexNode> sharedPredicates = null;
      for (var scan : scans) {
        var predicates = directFilterPredicates.get(scan);
        if (predicates == null) {
          return List.of();
        }

        if (sharedPredicates == null) {
          sharedPredicates = new LinkedHashSet<>(predicates);
        } else {
          sharedPredicates.retainAll(predicates);
        }
      }

      if (sharedPredicates == null) {
        return List.of();
      }

      return sharedPredicates.stream().filter(RexUtil::isDeterministic).toList();
    }

    @Override
    public RelNode visit(TableScan scan) {
      scansByTableId.computeIfAbsent(getTableId(scan), ignored -> new ArrayDeque<>()).addLast(scan);
      return scan;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
      if (filter.getInput() instanceof TableScan scan) {
        directFilterPredicates.put(scan, RelOptUtil.conjunctions(filter.getCondition()));
      }
      filter.getCondition().accept(subQueryRexShuttle);
      return super.visit(filter);
    }

    @Override
    public RelNode visit(LogicalProject project) {
      project
          .getProjects()
          .forEach(projectExpression -> projectExpression.accept(subQueryRexShuttle));
      return super.visit(project);
    }
  }
}
