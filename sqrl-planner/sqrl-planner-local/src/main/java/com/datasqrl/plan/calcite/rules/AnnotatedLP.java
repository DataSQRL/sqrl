/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.calcite.rules;

import com.datasqrl.engine.EngineCapability;
import com.datasqrl.plan.calcite.hints.DedupHint;
import com.datasqrl.plan.calcite.table.NowFilter;
import com.datasqrl.plan.calcite.table.PullupOperator;
import com.datasqrl.plan.calcite.table.SortOrder;
import com.datasqrl.plan.calcite.table.TableType;
import com.datasqrl.plan.calcite.table.TimestampHolder;
import com.datasqrl.plan.calcite.table.TopNConstraint;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.plan.calcite.util.ContinuousIndexMap;
import com.datasqrl.plan.calcite.util.IndexMap;
import com.datasqrl.plan.calcite.util.SqrlRexUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ContiguousSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.ToString;
import lombok.Value;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

@Value
@AllArgsConstructor
@Builder
@ToString
public class AnnotatedLP implements RelHolder {

  @NonNull
  public RelNode relNode;
  @NonNull
  public TableType type;
  @NonNull
  public ContinuousIndexMap primaryKey;
  @NonNull
  public TimestampHolder.Derived timestamp;
  @NonNull
  public ContinuousIndexMap select;
  @Builder.Default
  public List<JoinTable> joinTables = null;
  @Builder.Default
  @NonNull
  public Optional<Integer> numRootPks = Optional.empty();

  @Builder.Default
  @NonNull
  public NowFilter nowFilter = NowFilter.EMPTY; //Applies before topN
  @Builder.Default
  @NonNull
  public TopNConstraint topN = TopNConstraint.EMPTY; //Applies before sort
  @Builder.Default
  @NonNull
  public SortOrder sort = SortOrder.EMPTY;

  @Builder.Default
  @NonNull
  public List<AnnotatedLP> inputs = List.of();

  public static AnnotatedLPBuilder build(RelNode relNode, TableType type,
      ContinuousIndexMap primaryKey,
      TimestampHolder.Derived timestamp, ContinuousIndexMap select,
      AnnotatedLP input) {
    return build(relNode, type, primaryKey, timestamp, select, List.of(input));
  }

  public static AnnotatedLPBuilder build(RelNode relNode, TableType type,
      ContinuousIndexMap primaryKey,
      TimestampHolder.Derived timestamp, ContinuousIndexMap select,
      List<AnnotatedLP> inputs) {
    return AnnotatedLP.builder().relNode(relNode).type(type).primaryKey(primaryKey)
        .timestamp(timestamp)
        .select(select).inputs(inputs);
  }

  public AnnotatedLPBuilder copy() {
    AnnotatedLPBuilder builder = AnnotatedLP.builder();
    builder.relNode(relNode);
    builder.type(type);
    builder.primaryKey(primaryKey);
    builder.timestamp(timestamp);
    builder.select(select);
    builder.joinTables(joinTables);
    builder.numRootPks(numRootPks);
    builder.nowFilter(nowFilter);
    builder.topN(topN);
    builder.sort(sort);
    builder.inputs(List.of(this));
    return builder;
  }

  public int getFieldLength() {
    return relNode.getRowType().getFieldCount();
  }

  /**
   * Called to inline the TopNConstraint on top of the input relation. This will inline a nowFilter
   * if present
   *
   * @return
   */
  public AnnotatedLP inlineTopN(RelBuilder relBuilder, ExecutionAnalysis exec) {
    if (topN.isEmpty()) {
      return this;
    }
    if (!nowFilter.isEmpty()) {
      return inlineNowFilter(relBuilder, exec).inlineTopN(relBuilder, exec);
    }
    Preconditions.checkArgument(nowFilter.isEmpty());

    relBuilder.push(relNode);

    SortOrder newSort = sort;
    if (!topN.isDeduplication() && !topN.isDistinct() && (!topN.hasPartition() || !topN.hasLimit())) {
      RelCollation collation = topN.getCollation();
      if (topN.hasLimit()) { //It's not partitioned, so straight forward order and limit
        if (topN.hasCollation()) {
          relBuilder.sort(collation);
        }
        relBuilder.limit(0, topN.getLimit());
        exec.require(EngineCapability.GLOBAL_SORT);
      } else { //Lift up sort and prepend partition (if any)
        newSort = newSort.ifEmpty(SortOrder.of(topN.getPartition(), collation));
      }
      return AnnotatedLP.build(relBuilder.build(), type, primaryKey, timestamp, select, this)
          .sort(newSort).build();
    } else { //distinct or (hasPartition and hasLimit)
      final RelDataType inputType = relBuilder.peek().getRowType();
      RexBuilder rexBuilder = relBuilder.getRexBuilder();

      List<Integer> projectIdx = ContiguousSet.closedOpen(0, inputType.getFieldCount()).asList();
      List<Integer> partitionIdx = topN.getPartition();

      int rowFunctionColumns = 1;
      if (topN.isDistinct()) { //It's a partitioned distinct
        rowFunctionColumns += topN.hasLimit() ? 2 : 1;
      }
      int projectLength = projectIdx.size() + rowFunctionColumns;

      //Create references for all projects and partition keys
      List<RexNode> partitionKeys = new ArrayList<>(partitionIdx.size());
      List<RexNode> projects = new ArrayList<>(projectLength);
      List<String> projectNames = new ArrayList<>(projectLength);
      for (Integer idx : projectIdx) {
        RexInputRef ref = RexInputRef.of(idx, inputType);
        projects.add(ref);
        projectNames.add(null);
        if (partitionIdx.contains(idx)) {
          partitionKeys.add(ref);
        }
      }
      assert projects.size() == projectIdx.size() && partitionKeys.size() == partitionIdx.size();

      List<RexFieldCollation> fieldCollations = new ArrayList<>();
      fieldCollations.addAll(SqrlRexUtil.translateCollation(topN.getCollation(), inputType));
      if (topN.isDistinct()) {
        //Add all other selects that are not partition indexes or collations to the sort
        List<Integer> remainingDistincts = new ArrayList<>(primaryKey.targetsAsList());
        topN.getCollation().getFieldCollations().stream().map(RelFieldCollation::getFieldIndex)
            .forEach(remainingDistincts::remove);
        topN.getPartition().stream().forEach(remainingDistincts::remove);
        remainingDistincts.stream().map(idx -> new RexFieldCollation(RexInputRef.of(idx, inputType),
                Set.of(SqlKind.NULLS_LAST)))
            .forEach(fieldCollations::add);
      }

      SqrlRexUtil rexUtil = new SqrlRexUtil(relBuilder.getTypeFactory());
      //Add row_number (since it always applies)
      projects.add(rexUtil.createRowFunction(SqlStdOperatorTable.ROW_NUMBER, partitionKeys,
          fieldCollations));
      projectNames.add(null);
      int rowNumberIdx = projectIdx.size(), rankIdx = rowNumberIdx + 1, denserankIdx =
          rowNumberIdx + 2;
      if (topN.isDistinct()) {
        //Add rank and dense_rank if we have a limit
        projects.add(
            rexUtil.createRowFunction(SqlStdOperatorTable.RANK, partitionKeys, fieldCollations));
        projectNames.add(null);
        if (topN.hasLimit()) {
          projects.add(rexUtil.createRowFunction(SqlStdOperatorTable.DENSE_RANK, partitionKeys,
              fieldCollations));
          projectNames.add(null);
        }
        exec.require(EngineCapability.MULTI_RANK);
      }

      relBuilder.project(projects, projectNames);
      RelDataType windowType = relBuilder.peek().getRowType();
      //Add filter
      List<RexNode> conditions = new ArrayList<>();
      if (topN.isDistinct()) {
        conditions.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            RexInputRef.of(rowNumberIdx, windowType),
            RexInputRef.of(rankIdx, windowType)));
        if (topN.hasLimit()) {
          conditions.add(
              SqrlRexUtil.makeWindowLimitFilter(rexBuilder, topN.getLimit(), denserankIdx,
                  windowType));
        }
      } else {
        conditions.add(
            SqrlRexUtil.makeWindowLimitFilter(rexBuilder, topN.getLimit(), rowNumberIdx, windowType));
      }

      relBuilder.filter(conditions);
      ContinuousIndexMap newPk = primaryKey.remap(IndexMap.IDENTITY);
      ContinuousIndexMap newSelect = select.remap(IndexMap.IDENTITY);
      if (topN.isDeduplication() || topN.isDistinct()) { //Drop sort since it doesn't apply globally
        newSort = SortOrder.EMPTY;
      } else { //Add partitioned sort on top
        SortOrder sortByRowNum = SortOrder.of(topN.getPartition(),
            RelCollations.of(new RelFieldCollation(rowNumberIdx,
                RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST)));
        newSort = newSort.ifEmpty(sortByRowNum);
      }
      if (topN.isDeduplication()) { //Add hint for physical plan analysis
        DedupHint.of().addTo(relBuilder);
      }
      return AnnotatedLP.build(relBuilder.build(), type, newPk, timestamp, newSelect, this)
          .sort(newSort).build();
    }
  }

  public AnnotatedLP inlineNowFilter(RelBuilder relB, ExecutionAnalysis exec) {
    if (nowFilter.isEmpty()) {
      return this;
    }
    exec.require(EngineCapability.NOW);
    nowFilter.addFilterTo(relB.push(relNode));
    return copy().relNode(relB.build())
        .nowFilter(NowFilter.EMPTY).build();

  }

  public AnnotatedLP inlineSort(RelBuilder relB, ExecutionAnalysis exec) {
    if (sort.isEmpty()) {
      return this;
    }
    //Need to inline now-filter and topN first
    if (!nowFilter.isEmpty()) {
      return inlineNowFilter(relB, exec).inlineSort(relB, exec);
    }
    if (!topN.isEmpty()) {
      return inlineTopN(relB, exec).inlineSort(relB, exec);
    }
    exec.require(EngineCapability.GLOBAL_SORT);
    sort.addTo(relB.push(relNode));
    return copy().relNode(relB.build())
        .sort(SortOrder.EMPTY).build();
  }

  public AnnotatedLP dropSort() {
    return copy().sort(SortOrder.EMPTY).build();
  }

  public PullupOperator.Container getPullups() {
    return new PullupOperator.Container(nowFilter, topN, sort);
  }

  public AnnotatedLP withDefaultSort() {
    SortOrder newSort;
    if (sort.isEmpty()) {
      newSort = getDefaultOrder(this);
    } else {
      newSort = sort.ensurePrimaryKeyPresent(primaryKey);
      if (newSort.equals(sort)) {
        return this;
      }
    }
    return copy().sort(newSort).build();
  }

  public SortOrder getDefaultOrder(AnnotatedLP alp) {
    //If stream, timestamp first then pk, otherwise just pk
    List<RelFieldCollation> collations = new ArrayList<>();
    if (alp.getType().isStream() && alp.getTimestamp().hasFixedTimestamp()) {
      collations.add(new RelFieldCollation(alp.getTimestamp().getTimestampCandidate().getIndex(),
          RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST));
    }
    return new SortOrder(RelCollations.of(collations)).ensurePrimaryKeyPresent(alp.primaryKey);
  }


  /**
   * Moves the primary key columns to the front and adds projection to only return columns that the
   * user selected, are part of the primary key, a timestamp candidate, or part of the sort order.
   * <p>
   * Inlines deduplication in case of nested data.
   *
   * @return
   */
  public AnnotatedLP postProcess(@NonNull RelBuilder relBuilder, List<String> fieldNames,
      ExecutionAnalysis exec) {
    if (fieldNames == null) { //Use existing fieldnames
      fieldNames = Collections.nCopies(select.getSourceLength(), null);
    }
    Preconditions.checkArgument(fieldNames.size() == select.getSourceLength());
    List<RelDataTypeField> fields = relNode.getRowType().getFieldList();
    AnnotatedLP input = this;
    if (!topN.isEmpty() && //If any selected field is nested we have to inline topN
        select.targetsAsList().stream().map(fields::get).map(RelDataTypeField::getType)
            .anyMatch(CalciteUtil::isNestedTable)) {
      input = input.inlineTopN(relBuilder, exec);
    }
    HashMap<Integer, Integer> remapping = new HashMap<>();
    int index = 0;
    boolean addedPk = false;
    for (int i = 0; i < input.primaryKey.getSourceLength(); i++) {
      remapping.put(input.primaryKey.map(i), index++);
    }
    if (input.primaryKey.getSourceLength() == 0) {
      //If we don't have a primary key, we add a static one to resolve uniqueness in the database
      addedPk = true;
      index++;
    }
    for (int i = 0; i < input.select.getSourceLength(); i++) {
      int target = input.select.map(i);
      if (!remapping.containsKey(target)) {
        remapping.put(target, index++);
      }
    }

    //Determine which timestamp candidates have already been mapped and map the candidates accordingly
    List<TimestampHolder.Derived.Candidate> candidates = new ArrayList<>();
    for (TimestampHolder.Derived.Candidate c : input.timestamp.getCandidates()) {
      Integer mappedIndex;
      if ((mappedIndex = remapping.get(c.getIndex()))!=null) {
        candidates.add(c.withIndex(mappedIndex));
      }
    }
    if (candidates.isEmpty()) {
      //if no timestamp candidate has been mapped, map the best one to preserve a timestamp
      TimestampHolder.Derived.Candidate bestCandidate = input.timestamp.getBestCandidate();
      int nextIndex = index++;
      remapping.put(bestCandidate.getIndex(), nextIndex);
      candidates.add(bestCandidate.withIndex(nextIndex));
    }

    //Make sure we preserve sort orders if they aren't selected
    for (RelFieldCollation fieldcol : input.sort.getCollation().getFieldCollations()) {
      if (!remapping.containsKey(fieldcol.getFieldIndex())) {
        remapping.put(fieldcol.getFieldIndex(), index++);
      }
    }

    int projectLength = index;
    int inputLength = input.getFieldLength();
    Preconditions.checkArgument(remapping.keySet().stream().allMatch(idx -> idx < inputLength)
        && remapping.size() + (addedPk ? 1 : 0) == index && projectLength <= inputLength + (addedPk
        ? 1 : 0));
    IndexMap remap = IndexMap.of(remapping);
    ContinuousIndexMap updatedSelect = input.select.remap(remap);
    List<RexNode> projects = new ArrayList<>(projectLength);
    List<String> updatedFieldNames = Arrays.asList(new String[projectLength]);
    ContinuousIndexMap primaryKey = input.primaryKey.remap(remap);
    if (addedPk) {
      primaryKey = ContinuousIndexMap.identity(1, projectLength);
      projects.add(0, relBuilder.literal(1));
      updatedFieldNames.set(0, SQRLLogicalPlanRewriter.DEFAULT_PRIMARY_KEY_COLUMN_NAME);
    }
    RelDataType rowType = input.relNode.getRowType();
    remapping.entrySet().stream().map(e -> new IndexMap.Pair(e.getKey(), e.getValue()))
        .sorted((a, b) -> Integer.compare(a.getTarget(), b.getTarget()))
        .forEach(p -> {
          projects.add(p.getTarget(), RexInputRef.of(p.getSource(), rowType));
        });
    for (int i = 0; i < fieldNames.size(); i++) {
      updatedFieldNames.set(updatedSelect.map(i), fieldNames.get(i));
    }
    relBuilder.push(input.relNode);
    relBuilder.project(projects, updatedFieldNames, true); //Force to make sure fields are renamed
    RelNode relNode = relBuilder.build();

    return new AnnotatedLP(relNode, input.type, primaryKey,
        input.timestamp.restrictTo(candidates), updatedSelect,null,
        input.numRootPks,
        input.nowFilter.remap(remap), input.topN.remap(remap), input.sort.remap(remap),
        List.of(this));
  }


  public double estimateRowCount() {
    final RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();
    return mq.getRowCount(relNode);
    //return 1.0d;
  }


}
