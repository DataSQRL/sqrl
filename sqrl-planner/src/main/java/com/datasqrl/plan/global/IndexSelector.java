/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.global;

import com.datasqrl.plan.calcite.RelStageRunner;
import com.datasqrl.plan.calcite.SqrlPlannerConfigFactory;
import com.datasqrl.plan.calcite.rules.SQRLLogicalPlanConverter;
import com.datasqrl.util.ArrayUtil;
import com.datasqrl.plan.calcite.OptimizationStage;
import com.datasqrl.plan.calcite.table.VirtualRelationalTable;
import com.datasqrl.plan.calcite.util.SqrlRexUtil;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Ints;
import lombok.AllArgsConstructor;
import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.adapter.enumerable.EnumerableNestedLoopJoin;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.commons.math3.util.Precision;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.datasqrl.plan.calcite.OptimizationStage.READ_QUERY_OPTIMIZATION;

@AllArgsConstructor
public class IndexSelector {

  private static final double EPSILON = 0.00001d;

  private final RelOptPlanner planner;
  private final IndexSelectorConfig config;

  public List<IndexCall> getIndexSelection(OptimizedDAG.ReadQuery query) {
    RelNode optimized = RelStageRunner.runStage(READ_QUERY_OPTIMIZATION, query.getRelNode(), planner);
//        System.out.println(optimized.explain());
    IndexFinder indexFinder = new IndexFinder();
    return indexFinder.find(optimized);
  }

  public Map<IndexDefinition, Double> optimizeIndexes(Collection<IndexCall> indexes) {
    //Prune down to database indexes and remove duplicates
    Map<IndexDefinition, Double> optIndexes = new HashMap<>();
    Multimap<VirtualRelationalTable, IndexCall> callsByTable = HashMultimap.create();
    indexes.forEach(idx -> callsByTable.put(idx.getTable(), idx));

    for (VirtualRelationalTable table : callsByTable.keySet()) {
      optIndexes.putAll(optimizeIndexes(table, callsByTable.get(table)));
    }
    return optIndexes;
  }

  private Map<IndexDefinition, Double> optimizeIndexes(VirtualRelationalTable table,
                                                       Collection<IndexCall> indexes) {
    //Check how many unique column combinations we have on this table
    Set<Set<Integer>> columnIndexSets = indexes.stream().map(idx -> idx.getColumnIndexes()).collect(Collectors.toSet());
    if (columnIndexSets.size()>config.maxIndexColumnSets()) {
      //Generate individual indexes so the database can combine them on-demand at query time
      Set<Integer> indexedColumns = columnIndexSets.stream().flatMap(s -> s.stream()).collect(Collectors.toSet());
      IndexDefinition.Type genericType = config.getPreferredGenericIndexType();
      return indexedColumns.stream().map(
              col -> new IndexDefinition(table.getNameId(), List.of(col), table.getRowType().getFieldNames(), genericType)
      ).collect(Collectors.toMap(Function.identity(),x -> 0.0));
    } else {
      return optimizeIndexesWithCostMinimization(table, indexes);
    }
  }

  private Map<IndexDefinition, Double> optimizeIndexesWithCostMinimization(VirtualRelationalTable table,
      Collection<IndexCall> indexes) {
    Map<IndexDefinition, Double> optIndexes = new HashMap<>();
    //The baseline cost is the cost of doing the lookup with the primary key index
    Map<IndexCall, Double> currentCost = new HashMap<>();
    IndexDefinition pkIdx = IndexDefinition.getPrimaryKeyIndex(table.getNameId(),
        table.getNumPrimaryKeys(), table.getRowType().getFieldNames());
    for (IndexCall idx : indexes) {
      currentCost.put(idx, idx.getCost(pkIdx));
    }
    //Determine which index candidates reduce the cost the most
    Set<IndexDefinition> candidates = new HashSet<>();
    indexes.forEach(idx -> candidates.addAll(generateIndexCandidates(idx)));
    candidates.remove(pkIdx);
    double beforeTotal = total(currentCost);
    for (; ; ) {
      IndexDefinition bestCandidate = null;
      Map<IndexCall, Double> bestCosts = null;
      double bestTotal = Double.POSITIVE_INFINITY;
      for (IndexDefinition candidate : candidates) {
        Map<IndexCall, Double> costs = new HashMap<>();
        currentCost.forEach((call, cost) -> {
          double newcost = call.getCost(candidate);
            if (newcost > cost) {
                newcost = cost;
            }
          costs.put(call, newcost);
        });
        double total = total(costs);
        if (total < beforeTotal && (total + EPSILON < bestTotal ||
            (Precision.equals(total,bestTotal, 2*EPSILON) && costLess(candidate,bestCandidate)))) {
          bestCandidate = candidate;
          bestCosts = costs;
          bestTotal = total;
        }
      }
      if (bestCandidate != null
          && bestTotal / beforeTotal <= config.getCostImprovementThreshold()) {
        optIndexes.put(bestCandidate, beforeTotal - bestTotal);
        candidates.remove(bestCandidate);
        beforeTotal = bestTotal;
        currentCost = bestCosts;
      } else {
        break;
      }
    }
    return optIndexes;
  }

  private boolean costLess(IndexDefinition candidate, IndexDefinition bestCandidate) {
    double cost = config.relativeIndexCost(candidate);
    double bestcost = config.relativeIndexCost(bestCandidate);
    if (cost + EPSILON < bestcost) return true;
    else if (Precision.equals(cost,bestcost,2*EPSILON)) {
      //Make index selection deterministic by prefering smaller columns
      return orderingScore(candidate) < orderingScore(bestCandidate);
    } else return false;
  }

  private int orderingScore(IndexDefinition candidate) {
    int score = 0;
    for (Integer column : candidate.getColumns()) {
      score = score*2 + column;
    }
    return score;
  }

  private double relativeIndexCost(IndexDefinition index) {
    return config.relativeIndexCost(index) + epsilon(
        index.getColumns()); //Add an epsilon that is insignificant but keeps index order stable
  }

  private static final double total(Map<?, Double> costs) {
    return costs.values().stream().reduce(0.0d, (a, b) -> a + b);
  }

  public Set<IndexDefinition> generateIndexCandidates(IndexCall indexCall) {
    List<Integer> eqCols = new ArrayList<>(), comparisons = new ArrayList<>();
    indexCall.getColumns().forEach(c -> {
      switch (c.getType()) {
        case EQUALITY:
          eqCols.add(c.getColumnIndex());
          break;
        case COMPARISON:
          comparisons.add(c.getColumnIndex());
          break;
        default:
          throw new IllegalStateException(c.getType().name());
      }
    });
    Set<IndexDefinition> result = new HashSet<>();

    for (IndexDefinition.Type indexType : config.supportedIndexTypes()) {
      List<List<Integer>> colPermutations = new ArrayList<>();
      switch (indexType) {
        case HASH:
          generatePermutations(new int[Math.min(eqCols.size(), config.maxIndexColumns(indexType))],
              0, eqCols, List.of(), colPermutations);
          break;
        case BTREE:
          generatePermutations(new int[Math.min(eqCols.size() + (comparisons.isEmpty() ? 0 : 1),
                  config.maxIndexColumns(indexType))],
              0, eqCols, comparisons, colPermutations);
          break;
        default:
          throw new IllegalStateException(indexType.name());
      }
      colPermutations.forEach(
          cols -> result.add(new IndexDefinition(indexCall.getTable().getNameId(), cols,
              indexCall.getTable().getRowType().getFieldNames(), indexType)));
    }
    return result;
  }


  private void generatePermutations(int[] selected, int depth, List<Integer> eqCols,
      List<Integer> comparisons, Collection<List<Integer>> permutations) {
    if (depth >= selected.length) {
      permutations.add(Ints.asList(selected.clone()));
      return;
    }
    if (depth >= eqCols.size()) {
      for (int comp : comparisons) {
        selected[depth] = comp;
        generatePermutations(selected, depth + 1, eqCols, comparisons, permutations);
      }
    }
    for (int eq : eqCols) {
        if (ArrayUtil.contains(selected, eq, depth)) {
            continue;
        }
      selected[depth] = eq;
      generatePermutations(selected, depth + 1, eqCols, comparisons, permutations);
    }
  }

  static final double epsilon(List<Integer> columns) {
    long eps = 0;
    for (int col : columns) {
      eps = eps * 2 + col;
    }
    return eps * 1e-5;
  }

  class IndexFinder extends RelVisitor {

    private static final int PARAM_OFFSET = 10000;

    List<IndexCall> indexes = new ArrayList<>();
    int paramIndex = PARAM_OFFSET;
    SqrlRexUtil rexUtil = new SqrlRexUtil(SqrlPlannerConfigFactory.createSqrlTypeFactory());

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {
      if (node instanceof EnumerableNestedLoopJoin) {
        EnumerableNestedLoopJoin join = (EnumerableNestedLoopJoin) node;
        visit(join.getLeft(), 0, node);
        RelNode right = join.getRight();
        //Push join filter into right
        RexNode nestedCondition = pushJoinConditionIntoRight(join);
        right = EnumerableFilter.create(right, nestedCondition);
        right = RelStageRunner.runStage(OptimizationStage.PUSH_DOWN_FILTERS, right, planner);
        visit(right, 1, node);
      } else if (node instanceof TableScan && parent instanceof Filter) {
        VirtualRelationalTable table = ((TableScan) node).getTable()
            .unwrap(VirtualRelationalTable.class);
        Filter filter = (Filter) parent;
        IndexCall.of(table, filter.getCondition(), rexUtil).map(indexes::add);
      } else {
        super.visit(node, ordinal, parent);
      }
    }


    private RexNode pushJoinConditionIntoRight(Join join) {
      return join.getCondition()
          .accept(new JoinConditionRewriter(join.getLeft().getRowType().getFieldCount(),
              join.getRight()));
    }

    List<IndexCall> find(RelNode node) {
      go(node);
      return indexes;
    }

    @AllArgsConstructor
    class JoinConditionRewriter extends RexShuttle {

      final int maxLeftIdx;
      final RelNode right;

      @Override
      public RexNode visitInputRef(RexInputRef ref) {
        if (ref.getIndex() < maxLeftIdx) {
          //Replace with variables
          return new RexDynamicParam(ref.getType(), paramIndex++);
        } else {
          //Shift indexes
          return RexInputRef.of(ref.getIndex() - maxLeftIdx, right.getRowType());
        }
      }

    }

  }

}
