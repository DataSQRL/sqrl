/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.global;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.function.IndexType;
import com.datasqrl.plan.OptimizationStage;
import com.datasqrl.plan.RelStageRunner;
import com.datasqrl.plan.global.QueryIndexSummary.IndexableFunctionCall;
import com.datasqrl.plan.hints.IndexHint;
import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.datasqrl.plan.table.PhysicalTable;
import com.datasqrl.plan.table.QueryRelationalTable;
import com.datasqrl.util.ArrayUtil;
import com.datasqrl.calcite.SqrlRexUtil;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.primitives.Ints;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.adapter.enumerable.EnumerableNestedLoopJoin;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.commons.math3.util.Precision;

import java.util.*;

import static com.datasqrl.plan.OptimizationStage.READ_QUERY_OPTIMIZATION;

@AllArgsConstructor
public class IndexSelector {

  private static final double EPSILON = 0.00001d;

  private static final int MAX_LIMIT_INDEX_SCAN = 10000;

  private final SqrlFramework framework;
  private final IndexSelectorConfig config;

  public List<QueryIndexSummary> getIndexSelection(PhysicalDAGPlan.ReadQuery query) {
    RelNode optimized = RelStageRunner.runStage(READ_QUERY_OPTIMIZATION, query.getRelNode(), framework.getQueryPlanner()
        .getPlanner());
    IndexFinder indexFinder = new IndexFinder();
    return indexFinder.find(optimized);
  }

  public Map<IndexDefinition, Double> optimizeIndexes(Collection<QueryIndexSummary> queryIndexSummaries) {
    //Prune down to database indexes and remove duplicates
    Map<IndexDefinition, Double> optIndexes = new HashMap<>();
    LinkedHashMultimap<PhysicalRelationalTable, QueryIndexSummary> callsByTable = LinkedHashMultimap.create();
    queryIndexSummaries.forEach(idx -> {
      //TODO: Add up counts so we preserve relative frequency
      callsByTable.put(idx.getTable(), idx);
    });

    for (PhysicalRelationalTable table : callsByTable.keySet()) {
      optIndexes.putAll(optimizeIndexes(table, callsByTable.get(table)));
    }
    return optIndexes;
  }

  private static IndexDefinition getIndexFromHint(PhysicalRelationalTable table, IndexHint hint) {
    List<String> colNames = hint.getColumnNames();
    List<Integer> colIdx = hint.getColumnNames().stream().map(colName -> {
      RelDataTypeField field = table.getRowType().getField(colName, false, true);
      Preconditions.checkArgument(field!=null, "Could not find indexed field %s for table %s in index hint %s", colName, table, hint);
      return field.getIndex();
    }).collect(Collectors.toUnmodifiableList());
    return new IndexDefinition(table.getNameId(), colIdx, table.getRowType().getFieldNames(),
        hint.getIndexType().isPartitioned()? colNames.size() : -1, hint.getIndexType());
  }

  public Optional<List<IndexDefinition>> getIndexHints(PhysicalRelationalTable table) {
    List<IndexHint> indexHints = StreamUtil.filterByClass(table.getOptimizerHints(), IndexHint.class)
        .collect(Collectors.toUnmodifiableList());
    if (!indexHints.isEmpty()) {
      return Optional.of(indexHints.stream().filter(IndexHint::isValid)
          .filter(idxHint -> config.supportedIndexTypes().contains(idxHint.getIndexType()))
          .map(idxHint -> getIndexFromHint(table, idxHint))
          .collect(Collectors.toUnmodifiableList()));
    } else {
      return Optional.empty();
    }
  }

  private Map<IndexDefinition, Double> optimizeIndexes(PhysicalRelationalTable table,
                                                       Set<QueryIndexSummary> queryIndexSummaries) {
    //Check how many unique QueryConjunctions we have on this table
    if (queryIndexSummaries.size()>config.maxIndexColumnSets()) {
      //Generate individual indexes so the database can combine them on-demand at query time
      //1) Generate an index for each column
      Set<Integer> indexedColumns = new HashSet<>();
      Set<IndexableFunctionCall> indexedFunctions = new HashSet<>();
      for (QueryIndexSummary conj : queryIndexSummaries) {
        indexedColumns.addAll(conj.equalityColumns);
        indexedColumns.addAll(conj.inequalityColumns);
        indexedFunctions.addAll(conj.functionCalls);
      }
      //Remove first primary key column
      indexedColumns.remove(0);
      //Pick generic index type
      IndexType genericType = config.getPreferredGenericIndexType();
      Map<IndexDefinition, Double> indexes = new HashMap<>();
      for (int colIndex : indexedColumns) {
        indexes.put(new IndexDefinition(table.getNameId(), List.of(colIndex),
            table.getRowType().getFieldNames(), -1, genericType), 0.0);
      }
      indexedFunctions.stream().map(fcall -> getIndexDefinition(fcall, table)).flatMap(Optional::stream)
          .forEach(idxDef -> indexes.put(idxDef, Double.NaN));
      return indexes;
    } else {
      return optimizeIndexesWithCostMinimization(table, queryIndexSummaries);
    }
  }

  private Optional<IndexDefinition> getIndexDefinition(IndexableFunctionCall fcall, PhysicalRelationalTable table) {
    Optional<IndexType> specialType = config.getPreferredSpecialIndexType(fcall.getFunction()
        .getSupportedIndexes());
    return specialType.map(idxType -> new IndexDefinition(table.getNameId(), fcall.getColumnIndexes(),
        table.getRowType().getFieldNames(), -1, idxType));
  }

  private Map<IndexDefinition, Double> optimizeIndexesWithCostMinimization(
      PhysicalRelationalTable table,
      Collection<QueryIndexSummary> indexes) {
    Map<IndexDefinition, Double> optIndexes = new HashMap<>();
    //Determine all index candidates
    Set<IndexDefinition> candidates = new LinkedHashSet<>();
    indexes.forEach(idx -> candidates.addAll(generateIndexCandidates(idx)));
    Function<QueryIndexSummary, Double> initialCost = idx -> idx.getBaseCost();
    if (config.hasPrimaryKeyIndex()) {
      //The baseline cost is the cost of doing the lookup with the primary key index
      IndexDefinition pkIdx = IndexDefinition.getPrimaryKeyIndex(table.getNameId(),
          table.getPrimaryKey().asList(), table.getRowType().getFieldNames());
      initialCost = idx -> idx.getCost(pkIdx);
      candidates.remove(pkIdx);
    }
    //Set initial costs
    Map<QueryIndexSummary, Double> currentCost = new HashMap<>();
    for (QueryIndexSummary idx : indexes) {
      currentCost.put(idx, initialCost.apply(idx));
    }
    //Determine which index candidates reduce the cost the most
    double beforeTotal = total(currentCost);
    for (; ; ) {
      if (optIndexes.size() >= config.maxIndexes()) {
        break;
      }
      IndexDefinition bestCandidate = null;
      Map<QueryIndexSummary, Double> bestCosts = null;
      double bestTotal = Double.POSITIVE_INFINITY;
      for (IndexDefinition candidate : candidates) {
        Map<QueryIndexSummary, Double> costs = new HashMap<>();
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

  private static double total(Map<?, Double> costs) {
    return costs.values().stream().reduce(0.0d, Double::sum);
  }

  public Set<IndexDefinition> generateIndexCandidates(QueryIndexSummary queryIndexSummary) {
    List<Integer> eqCols = ImmutableList.copyOf(queryIndexSummary.equalityColumns),
        inequality = ImmutableList.copyOf(queryIndexSummary.inequalityColumns);
    Set<IndexDefinition> result = new LinkedHashSet<>();

    for (IndexType indexType : config.supportedIndexTypes()) {
      List<List<Integer>> colPermutations = new ArrayList<>();
      int maxIndexCols = eqCols.size();
      switch (indexType) {
        case HASH:
          maxIndexCols = Math.min(maxIndexCols, config.maxIndexColumns(indexType));
          if (maxIndexCols>0) {
            generatePermutations(new int[maxIndexCols],
                0, eqCols, List.of(), colPermutations);
          }
          break;
        case BTREE:
        case PBTREE:
          maxIndexCols = Math.min(maxIndexCols + (inequality.isEmpty() ? 0 : 1),
              config.maxIndexColumns(indexType));
          if (maxIndexCols>0) {
            generatePermutations(new int[maxIndexCols],
                0, eqCols, inequality, colPermutations);
          }
          break;
        case TEXT:
        case VEC_COSINE:
        case VEC_EUCLID:
          queryIndexSummary.functionCalls.stream().map(fcall -> this.getIndexDefinition(fcall,
              queryIndexSummary.getTable())).flatMap(Optional::stream).forEach(result::add);
          break;
        default:
          throw new IllegalStateException(indexType.name());
      }
      if (indexType.isPartitioned()) {
        colPermutations.forEach( cols -> {
          for (int i = 0; i <= cols.size(); i++) {
            result.add(new IndexDefinition(queryIndexSummary.getTable().getNameId(), cols,
                queryIndexSummary.getTable().getRowType().getFieldNames(), i, indexType));
          }

            });
      } else {
        colPermutations.forEach(
            cols -> result.add(new IndexDefinition(queryIndexSummary.getTable().getNameId(), cols,
                queryIndexSummary.getTable().getRowType().getFieldNames(), -1, indexType)));
      }
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

    List<QueryIndexSummary> queryIndexSummaries = new ArrayList<>();
    int paramIndex = PARAM_OFFSET;
    SqrlRexUtil rexUtil = new SqrlRexUtil(framework.getTypeFactory());

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {
      if (node instanceof EnumerableNestedLoopJoin) {
        EnumerableNestedLoopJoin join = (EnumerableNestedLoopJoin) node;
        visit(join.getLeft(), 0, node);
        RelNode right = join.getRight();
        //Push join filter into right
        RexNode nestedCondition = pushJoinConditionIntoRight(join);
        right = EnumerableFilter.create(right, nestedCondition);
        right = RelStageRunner.runStage(OptimizationStage.PUSH_DOWN_FILTERS, right, framework.getQueryPlanner()
            .getPlanner());
        visit(right, 1, node);
      } else if (node instanceof TableScan && parent instanceof Filter) {
        PhysicalRelationalTable table = ((TableScan) node).getTable()
            .unwrap(PhysicalRelationalTable.class);
        Filter filter = (Filter) parent;
        QueryIndexSummary.ofFilter(table, filter.getCondition(), rexUtil).map(queryIndexSummaries::add);
      } else if (node instanceof TableScan && parent instanceof Sort) {
        PhysicalRelationalTable table = ((TableScan) node).getTable()
            .unwrap(PhysicalRelationalTable.class);
        Sort sort = (Sort) parent;
        Optional<Integer> firstCollationIdx = getFirstCollation(sort);
        if (firstCollationIdx.isPresent() && hasLimit(sort)) {
          QueryIndexSummary.ofSort(table, firstCollationIdx.get()).map(queryIndexSummaries::add);
        }
      } else if (node instanceof Project && parent instanceof Sort && node.getInput(0) instanceof TableScan) {
        PhysicalRelationalTable table = ((TableScan) node.getInput(0)).getTable()
            .unwrap(PhysicalRelationalTable.class);
        Sort sort = (Sort) parent;
        Optional<Integer> firstCollationIdx = getFirstCollation(sort);
        if (firstCollationIdx.isPresent() && hasLimit(sort)) {
          Project project = (Project) node;
          RexNode sortRex = project.getProjects().get(firstCollationIdx.get());
          QueryIndexSummary.ofSort(table, sortRex).map(queryIndexSummaries::add);
        }
      } else {
        super.visit(node, ordinal, parent);
      }
    }

    private boolean hasLimit(Sort sort) {
      //Check for limit. Can only use index scans if there is a limit, otherwise it's a table scan
      return SqrlRexUtil.getLimit(sort.fetch).filter(limit -> limit <= MAX_LIMIT_INDEX_SCAN).isPresent();
    }

    private Optional<Integer> getFirstCollation(Sort sort) {
      List<RelFieldCollation> fieldCollations = sort.collation.getFieldCollations();
      if (fieldCollations.isEmpty()) return Optional.empty();
      RelFieldCollation firstCollation = fieldCollations.get(0);
      return Optional.of(firstCollation.getFieldIndex());
    }


    private RexNode pushJoinConditionIntoRight(Join join) {
      return join.getCondition()
          .accept(new JoinConditionRewriter(join.getLeft().getRowType().getFieldCount(),
              join.getRight()));
    }

    List<QueryIndexSummary> find(RelNode node) {
      go(node);
      return queryIndexSummaries;
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
