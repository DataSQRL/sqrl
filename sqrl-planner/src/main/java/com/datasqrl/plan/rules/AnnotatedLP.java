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
import com.datasqrl.io.tables.TableType;
import com.datasqrl.plan.hints.DedupHint;
import com.datasqrl.plan.hints.SqrlHint;
import com.datasqrl.plan.hints.TopNHint;
import com.datasqrl.plan.table.*;
import com.datasqrl.plan.table.Timestamps.Type;
import com.datasqrl.plan.util.SelectIndexMap;
import com.datasqrl.plan.util.PrimaryKeyMap;
import com.datasqrl.plan.util.IndexMap;
import com.datasqrl.calcite.SqrlRexUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ContiguousSet;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
import org.apache.calcite.rex.RexShuttle;
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

  public List<String> getFieldNamesWithIndex(List<Integer> indexes) {
    List<RelDataTypeField> alpFields = relNode.getRowType().getFieldList();
    return indexes.stream().map(i -> alpFields.get(i).getName() + "[" + i + "]").collect(Collectors.toList());
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
    if (topN.isDistinct() || (topN.hasPartition() && topN.hasLimit())) { //distinct or (hasPartition and hasLimit)
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
      if (topN.hasLimit(1) || topN.isDistinct()) { //Drop sort since it doesn't apply globally
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
    } else { //!distinct && (!hasPartition || !hasLimit)
      RelCollation collation = topN.getCollation();
      if (topN.hasLimit()) { //It's not partitioned, so straight forward global order and limit
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
    }
  }

  public AnnotatedLP inlineAll(RelBuilder relB, ExecutionAnalysis exec) {
    return this.inlineNowFilter(relB, exec).inlineTopN(relB, exec).inlineSort(relB, exec);
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

  private static final int PK_LITERAL_IDENTIFIER = -10;


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
      SqrlConverterConfig config, boolean inlinePullups, ErrorCollector errors) {
    errors.checkFatal(type!=TableType.LOOKUP, "Lookup tables can only be used in temporal joins");
    ExecutionAnalysis exec = config.getExecAnalysis();
    if (type==TableType.RELATION) exec.requireFeature(EngineFeature.RELATIONS);

    //Now filters are always inlined
    AnnotatedLP input = this;
    if (inlinePullups) {
      //Inline all pullups and don't pass them downstream in the DAG
      input = this.inlineAll(relBuilder, exec);
    }


    relBuilder.push(input.relNode);

    Function<Integer,RelDataTypeField> getField = i -> relBuilder.peek().getRowType().getFieldList().get(i);
    Function<Integer,String> getFieldName = i -> getField.apply(i).getName();

    List<String> originalFieldNames;
    NameAdjuster nameAdjuster;
    if (originalRelNode!=null && SqrlHint.fromRel(originalRelNode, TopNHint.CONSTRUCTOR)
            .filter(topN -> topN.getType()== TopNHint.Type.DISTINCT_ON).isEmpty()) {
      //We use the original field names because we may have lost them in processing
      originalFieldNames = originalRelNode.getRowType().getFieldNames();
      nameAdjuster = new NameAdjuster(originalFieldNames);
    } else {
      originalFieldNames = Collections.nCopies(input.select.getSourceLength(),null);
      nameAdjuster = new NameAdjuster(input.select.targetsAsList().stream().map(getFieldName)
              .collect(Collectors.toList()));
    }
    Preconditions.checkArgument(originalFieldNames.size()==input.select.getSourceLength());

    Function<Integer,String> makeMeta = i -> nameAdjuster.uniquifyName(Name.hiddenString(getFieldName.apply(i)));

    List<Integer> projectIndexes = new ArrayList<>();
    List<String> projectNames = new ArrayList<>();
    //Add all the selects first
    for (int i = 0; i < input.select.getSourceLength(); i++) {
      projectIndexes.add(input.select.map(i));
      projectNames.add(originalFieldNames.get(i));
    }
    //Add any primary key columns not already added and select the best one in case of duplicates
    PrimaryKeyMap primaryKey;
    if (config.primaryKeyNames!=null) {
      //The user is manually overwriting the primary key.
      List<Integer> chosenPks = new ArrayList<>();
      for (String pkName : config.primaryKeyNames) {
        OptionalInt pkIndex = IntStream.range(0, projectNames.size()).filter(i -> projectNames.get(i).equalsIgnoreCase(pkName)).findFirst();
        errors.checkFatal(pkIndex.isPresent(), "Primary key column %s not found in query", pkName);
        chosenPks.add(pkIndex.getAsInt());
      }
      primaryKey = PrimaryKeyMap.of(chosenPks);
    } else if (!input.primaryKey.isUndefined()) {
      List<Integer> chosenPks = new ArrayList<>();
      for (int i = 0; i < input.primaryKey.getLength(); i++) {
        PrimaryKeyMap.ColumnSet columnSet = input.primaryKey.get(i);
        //Find all indexes that map onto pk columns
        int[] newPkIndexes = IntStream.range(0, projectIndexes.size()).filter(idx -> columnSet.contains(projectIndexes.get(idx))).toArray();
        List<Integer> oldPkIndexes = Arrays.stream(newPkIndexes).mapToObj(projectIndexes::get).collect(Collectors.toList());
        if (newPkIndexes.length > 1) {
          errors.notice(MULTIPLE_PRIMARY_KEY, "A primary key column is mapped to multiple columns in query: %s",
                  oldPkIndexes.stream().map(getFieldName).collect(Collectors.toList()));
        }
        if (newPkIndexes.length == 0) {
          //Append pk column
          oldPkIndexes = columnSet.getIndexes().stream().sorted().collect(Collectors.toList());
        }
        assert !oldPkIndexes.isEmpty();
        //If we have multiple options for pk column, pick the first not-null one
        int oldChosenPkIdx = oldPkIndexes.stream().filter(idx -> !getField.apply(idx).getType().isNullable()).findFirst()
                .orElse(oldPkIndexes.get(0));
        if (getField.apply(oldChosenPkIdx).getType().isNullable()) {
          errors.warn(PRIMARY_KEY_NULLABLE, "The primary key field(s) [%s] are nullable",
                  oldPkIndexes.stream().map(getFieldName).collect(Collectors.joining(", ")));
        }
        int newChosenPkIdx;
        if (newPkIndexes.length == 0) {
          //Need to append
          newChosenPkIdx = projectIndexes.size();
          projectIndexes.add(oldChosenPkIdx);
          projectNames.add(makeMeta.apply(oldChosenPkIdx));
        } else {
          newChosenPkIdx = newPkIndexes[oldPkIndexes.indexOf(oldChosenPkIdx)];
        }
        chosenPks.add(newChosenPkIdx);
      }
      if (input.primaryKey.getLength() == 0 && config.addDefaultPrimaryKey) {
        //If we don't have a primary key, we add a static one to resolve uniqueness in the database
        chosenPks = List.of(projectIndexes.size());
        projectIndexes.add(PK_LITERAL_IDENTIFIER);
        projectNames.add(ReservedName.SYSTEM_PRIMARY_KEY.getDisplay());
      }
      primaryKey = PrimaryKeyMap.of(chosenPks);
    } else {
      primaryKey = PrimaryKeyMap.UNDEFINED;
    }

    //Determine which timestamp candidates have already been mapped and map the candidates accordingly
    Timestamps.TimestampsBuilder timestampBuilder;
    if (input.timestamp.is(Type.UNDEFINED)) {
      timestampBuilder = Timestamps.builder().type(Type.UNDEFINED);
    } else {
      timestampBuilder = Timestamps.builder().type(Type.OR);
      boolean mappedAny = false;
      for (Integer timeIdx : input.timestamp.getCandidates()) {
        int newIdx = projectIndexes.indexOf(timeIdx);
        if (newIdx >= 0) {
          timestampBuilder.candidate(newIdx);
          mappedAny = true;
          break;
        }
      }
      if (!mappedAny) {
        //if no timestamp candidate has been mapped, map the best one to preserve a timestamp
        Integer bestCandidate = input.timestamp.getBestCandidate(relBuilder);
        timestampBuilder.candidate(projectIndexes.size());
        projectIndexes.add(bestCandidate);
        projectNames.add(makeMeta.apply(bestCandidate));

      }
    }

    //Make sure we preserve sort orders if they aren't selected
    for (RelFieldCollation fieldcol : input.sort.getCollation().getFieldCollations()) {
      int newIdx = projectIndexes.indexOf(fieldcol.getFieldIndex());
      if (newIdx < 0) {
        projectIndexes.add(fieldcol.getFieldIndex());
        projectNames.add(makeMeta.apply(fieldcol.getFieldIndex()));
      }
    }

    Preconditions.checkArgument(projectIndexes.size()==projectNames.size());

    List<RexNode> projects = projectIndexes.stream().map(idx ->
            idx==PK_LITERAL_IDENTIFIER?relBuilder.literal(1):RexInputRef.of(idx, relBuilder.peek().getRowType()))
            .collect(Collectors.toUnmodifiableList());

    IndexMap remap = IndexMap.of(projectIndexes);
    SelectIndexMap updatedSelect = SelectIndexMap.identity(input.select.getSourceLength(), input.select.getSourceLength());

    //Only add projection if it adds anything, i.e. it changes what is selected or renames it
    List<String> peekedFieldNames = relBuilder.peek().getRowType().getFieldNames();
    if (!projectIndexes.equals(ContiguousSet.closedOpen(0, peekedFieldNames.size()).asList())
     || IntStream.range(0, peekedFieldNames.size()).filter(i ->projectNames.get(i)!=null)
        .anyMatch(i -> !projectNames.get(i).equals(peekedFieldNames.get(i)))) {
      relBuilder.project(projects, projectNames, true);
    }
    RelNode relNode = relBuilder.build();


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
