/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.rules;

import static com.datasqrl.error.ErrorCode.MULTIPLE_PRIMARY_KEY;
import static com.datasqrl.error.ErrorCode.PRIMARY_KEY_NULLABLE;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.hints.DedupHint;
import com.datasqrl.plan.table.*;
import com.datasqrl.plan.table.Timestamps.Type;
import com.datasqrl.plan.util.SelectIndexMap;
import com.datasqrl.plan.util.PrimaryKeyMap;
import com.datasqrl.plan.util.IndexMap;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.SqrlRexUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ContiguousSet;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.ToString;
import lombok.Value;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
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
  /* Metadata we keep track of for each RelNode */
  @NonNull
  public TableType type;
  @NonNull
  public PrimaryKeyMap primaryKey;
  @NonNull
  public Timestamps timestamp;
  @NonNull
  public SelectIndexMap select;
  /**
   * Used to detect whether the operator applies to a single stream record so we can optimize
   */
  @Builder.Default
  @NonNull
  public Optional<PhysicalRelationalTable> streamRoot = Optional.empty();

  /* Operators we pull up */
  @Builder.Default
  @NonNull
  public NowFilter nowFilter = NowFilter.EMPTY; //Applies before topN
  @Builder.Default
  @NonNull
  public TopNConstraint topN = TopNConstraint.EMPTY; //Applies before sort
  @Builder.Default
  @NonNull
  public SortOrder sort = SortOrder.EMPTY;

  /**
   * We keep track of the inputs for data lineage so we can track processing
   */
  @Builder.Default
  @NonNull
  public List<AnnotatedLP> inputs = List.of();

  public static AnnotatedLPBuilder build(RelNode relNode, TableType type,
      PrimaryKeyMap primaryKey, Timestamps timestamp, SelectIndexMap select,
      AnnotatedLP input) {
    return build(relNode, type, primaryKey, timestamp, select, List.of(input));
  }

  public static AnnotatedLPBuilder build(RelNode relNode, TableType type,
      PrimaryKeyMap primaryKey,
      Timestamps timestamp, SelectIndexMap select,
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
    builder.streamRoot(streamRoot);
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
    Preconditions.checkArgument(!timestamp.requiresInlining());
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
        exec.requireFeature(EngineFeature.GLOBAL_SORT);
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
        projectNames.add(relBuilder.peek().getRowType().getFieldNames().get(idx));
        if (partitionIdx.contains(idx)) {
          partitionKeys.add(ref);
        }
      }
      assert projects.size() == projectIdx.size() && partitionKeys.size() == partitionIdx.size();

      List<RexFieldCollation> fieldCollations = new ArrayList<>();
      fieldCollations.addAll(SqrlRexUtil.translateCollation(topN.getCollation(), inputType));
      if (topN.isDistinct()) {
        //Add all other selects that are not partition indexes or collations to the sort
        Set<Integer> excludedIndexes = topN.getCollation().getFieldCollations().stream().map(RelFieldCollation::getFieldIndex)
                .collect(Collectors.toSet());
        excludedIndexes.addAll(topN.getPartition());
        List<Integer> remainingDistincts = primaryKey.asList().stream()
                .filter(col -> !col.containsAny(excludedIndexes))
                .map(col -> col.pickBest(inputType))
                .collect(Collectors.toList());
        remainingDistincts.stream().map(idx -> new RexFieldCollation(RexInputRef.of(idx, inputType),
                Set.of(SqlKind.NULLS_LAST)))
            .forEach(fieldCollations::add);
      }

      SqrlRexUtil rexUtil = new SqrlRexUtil(relBuilder.getTypeFactory());
      //Add row_number (since it always applies)
      projects.add(rexUtil.createRowFunction(SqlStdOperatorTable.ROW_NUMBER, partitionKeys,
          fieldCollations));
      projectNames.add(Name.hiddenString("rownum"));
      int rowNumberIdx = projectIdx.size(), rankIdx = rowNumberIdx + 1, denserankIdx =
          rowNumberIdx + 2;
      if (topN.isDistinct()) {
        //Add rank and dense_rank if we have a limit
        projects.add(
            rexUtil.createRowFunction(SqlStdOperatorTable.RANK, partitionKeys, fieldCollations));
        projectNames.add(Name.hiddenString("rank"));
        if (topN.hasLimit()) {
          projects.add(rexUtil.createRowFunction(SqlStdOperatorTable.DENSE_RANK, partitionKeys,
              fieldCollations));
          projectNames.add(Name.hiddenString("denserank"));
        }
        exec.requireFeature(EngineFeature.MULTI_RANK);
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
      PrimaryKeyMap newPk = primaryKey.remap(IndexMap.IDENTITY);
      SelectIndexMap newSelect = select.remap(IndexMap.IDENTITY);
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
    exec.requireFeature(EngineFeature.NOW);
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
    exec.requireFeature(EngineFeature.GLOBAL_SORT);
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
    if (!topN.isEmpty()) return this;
    SortOrder newSort;
    if (sort.isEmpty()) {
      if (relNode instanceof LogicalSort) return this;
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
    if (alp.getType().isStream()) {
      collations.add(new RelFieldCollation(alp.getTimestamp().getOnlyCandidate(),
          RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST));
    }
    return new SortOrder(RelCollations.of(collations)).ensurePrimaryKeyPresent(alp.primaryKey);
  }

  public AnnotatedLP toRelation(RelBuilder relB, ExecutionAnalysis exec) {
    //inline everything
    AnnotatedLP inlined = inlineNowFilter(relB, exec).inlineTopN(relB, exec).inlineSort(relB, exec);
    //add select for select map
    relB.push(inlined.relNode);
    SqrlRexUtil rexUtil = new SqrlRexUtil(relB);
    relB.project(rexUtil.getProjection(inlined.select, relB.peek()));

    return AnnotatedLP.build(relB.build(), TableType.RELATION,
            PrimaryKeyMap.UNDEFINED, Timestamps.UNDEFINED,
            SelectIndexMap.identity(inlined.select.getSourceLength(), inlined.select.getSourceLength()),
            inlined).build();
  }


  /**
   * Postprocessing constructs the final RelNode to be almost identical to the user defined
   * projection/select plus additional metadata.
   *
   * Finalizes the RelNode by adding a projection for all the selected columns plus inferred
   * primary key and timestamp (if any) as well as sort orders.
   * This preserves the order of the select fields and may add additional fields at the end for
   * primary key and timestamp metadata as well as columns needed in the pulled up sort.
   *
   * Note, that we pick a single timestamp even when there are multiple viable candidates to
   * ensure that a table has a single timestamp. We either pick the first viable timestamp the
   * user selected or the best candidate according to {@link Timestamps#getBestCandidate(RelBuilder)}.
   *
   * @return An updated {@link AnnotatedLP} with an additional projection.
   */
  public AnnotatedLP postProcess(@NonNull RelBuilder relBuilder, RelNode originalRelNode,
      ExecutionAnalysis exec, ErrorCollector errors) {
    errors.checkFatal(type!=TableType.LOOKUP, "Lookup tables can only be used in temporal joins");

    AnnotatedLP input = this;
    relBuilder.push(input.relNode);

    HashMap<Integer, Integer> remapping = new HashMap<>();
    Set<Integer> isMetadata = new HashSet<>();
    int index = 0;
    //Add all the selects first
    for (int i = 0; i < input.select.getSourceLength(); i++) {
      remapping.put(input.select.map(i), index++);
    }
    //Add any primary key columns not already added
    for (int i = 0; i < input.primaryKey.getLength(); i++) {
      PrimaryKeyMap.ColumnSet columnSet = input.primaryKey.get(i);
      Set<Integer> mappedColumns = Sets.intersection(columnSet.getIndexes(), remapping.keySet());
      if (mappedColumns.isEmpty()) {
        //Pick smallest column index and append
        int pkIndex = columnSet.getIndexes().stream().sorted().findFirst().get();
        isMetadata.add(index);
        remapping.put(pkIndex, index++);
      }
    }

    int addedPkIndex;
    if (input.primaryKey.getLength() == 0) {
      //If we don't have a primary key, we add a static one to resolve uniqueness in the database
      isMetadata.add(index);
      addedPkIndex = index++;
    } else {
      addedPkIndex = -1;
    }
    //Determine which timestamp candidates have already been mapped and map the candidates accordingly
    Timestamps.TimestampsBuilder timestampBuilder = Timestamps.build(Type.OR);
    boolean mappedAny = false;
    for (Integer timeIdx : input.timestamp.getCandidates()) {
      Integer mappedIndex;
      if ((mappedIndex = remapping.get(timeIdx))!=null) {
        timestampBuilder.candidate(mappedIndex);
        mappedAny = true;
        break; //We only map the first timestamp if there are multiple
      }
    }
    if (!mappedAny) {
      //if no timestamp candidate has been mapped, map the best one to preserve a timestamp
      Integer bestCandidate = input.timestamp.getBestCandidate(relBuilder);
      int nextIndex = index++;
      isMetadata.add(nextIndex);
      remapping.put(bestCandidate, nextIndex);
      timestampBuilder.candidate(nextIndex);
    }

    //Make sure we preserve sort orders if they aren't selected
    for (RelFieldCollation fieldcol : input.sort.getCollation().getFieldCollations()) {
      if (!remapping.containsKey(fieldcol.getFieldIndex())) {
        isMetadata.add(index);
        remapping.put(fieldcol.getFieldIndex(), index++);
      }
    }

    int projectLength = index;
    RelDataType rowType = relBuilder.peek().getRowType();
    int inputLength = rowType.getFieldCount();
    List<RelDataTypeField> fieldList = rowType.getFieldList();
    Preconditions.checkArgument(remapping.keySet().stream().allMatch(idx -> idx < inputLength)
            && remapping.size() + (addedPkIndex>=0 ? 1 : 0) == index && projectLength <= inputLength + (addedPkIndex>=0
            ? 1 : 0), "Selected field counts don't add up");
    IndexMap remap = IndexMap.of(remapping);
    SelectIndexMap updatedSelect = input.select.remap(remap);
    Preconditions.checkArgument(updatedSelect.isIdentity(), "Something went wrong in mapping selected fields: %s", updatedSelect);

    List<RexNode> projects = new ArrayList<>(projectLength);
    List<String> updatedFieldNames = new ArrayList<>(projectLength);

    NameAdjuster nameAdjuster = new NameAdjuster(remapping.keySet().stream().map(i -> fieldList.get(i).getName())
            .collect(Collectors.toUnmodifiableList()));
    List<IndexMap.Pair> mappings = remapping.entrySet().stream().map(e -> new IndexMap.Pair(e.getKey(), e.getValue())).collect(Collectors.toList());
    if (addedPkIndex>=0) mappings.add(new IndexMap.Pair(-1,addedPkIndex));
    mappings.stream().sorted((a, b) -> Integer.compare(a.getTarget(), b.getTarget()))
            .forEach(p -> {
              int position = p.getTarget();
              int reference = p.getSource();
              if (position==addedPkIndex) {
                projects.add(position, relBuilder.literal(1));
                updatedFieldNames.add(position, ReservedName.SYSTEM_PRIMARY_KEY.getDisplay());
              } else {
                projects.add(position, RexInputRef.of(reference, rowType));
                if (isMetadata.contains(position)) {
                  //Rename field to "hide" it
                  String name = fieldList.get(reference).getName();
                  name = nameAdjuster.uniquifyName(Name.hiddenString(name));
                  updatedFieldNames.add(position, name);
                } else {
                  updatedFieldNames.add(position, null); //Preserve name
                }
              }
            });


    relBuilder.project(projects, updatedFieldNames);
    RelNode relNode = relBuilder.build();

    List<RelDataTypeField> fields = relNode.getRowType().getFieldList();
    Function<Integer,String> getFieldName = idx -> fields.get(idx).getName();

    PrimaryKeyMap primaryKey;
    if (addedPkIndex>=0) {
      primaryKey = PrimaryKeyMap.of(new int[]{addedPkIndex});
    } else {
      PrimaryKeyMap remappedPk = input.primaryKey.remap(remap);
      List<Integer> chosenColumns = remappedPk.asList().stream()
              .map(col -> {
                assert col.getIndexes().size()>0;
                if (col.getIndexes().size()>1) {
                  errors.warn(MULTIPLE_PRIMARY_KEY, "A primary key column is mapped to multiple columns in query: %s",
                          col.getIndexes().stream().map(getFieldName));
                }
                //Let's pick the first non-null version
                int chosen = col.pickBest(relNode.getRowType());
                if (fields.get(chosen).getType().isNullable()) {
                  errors.warn(PRIMARY_KEY_NULLABLE, "The primary key field(s) [%s] are nullable", col.getIndexes().stream().map(getFieldName));
                }
                return chosen;
              }).collect(Collectors.toUnmodifiableList());
      primaryKey = PrimaryKeyMap.of(chosenColumns);
    }

    return new AnnotatedLP(relNode, input.type, primaryKey,
        timestampBuilder.build(), updatedSelect,
        input.streamRoot,
        input.nowFilter.remap(remap), input.topN.remap(remap), input.sort.remap(remap),
        List.of(this));
  }


  public double estimateRowCount() {
    final RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();
    return mq.getRowCount(relNode);
    //return 1.0d;
  }


}
