/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.rules;

import static com.datasqrl.TimeFunctions.NOW;
import static com.datasqrl.error.ErrorCode.DISTINCT_ON_TIMESTAMP;
import static com.datasqrl.error.ErrorCode.NOT_YET_IMPLEMENTED;
import static com.datasqrl.error.ErrorCode.WRONG_INTERVAL_JOIN;
import static com.datasqrl.error.ErrorCode.WRONG_TABLE_TYPE;

import com.datasqrl.engine.EngineCapability;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.function.SqrlTimeTumbleFunction;
import com.datasqrl.plan.hints.JoinCostHint;
import com.datasqrl.plan.hints.SlidingAggregationHint;
import com.datasqrl.plan.hints.SqrlHint;
import com.datasqrl.plan.hints.SqrlHintStrategyTable;
import com.datasqrl.plan.hints.TemporalJoinHint;
import com.datasqrl.plan.hints.TopNHint;
import com.datasqrl.plan.hints.TumbleAggregationHint;
import com.datasqrl.plan.rel.LogicalStream;
import com.datasqrl.model.LogicalStreamMetaData;
import com.datasqrl.plan.rules.JoinTable.NormType;
import com.datasqrl.plan.rules.SQRLConverter.Config;
import com.datasqrl.plan.table.AddedColumn;
import com.datasqrl.plan.table.NowFilter;
import com.datasqrl.plan.table.PullupOperator;
import com.datasqrl.plan.table.ScriptRelationalTable;
import com.datasqrl.plan.table.SortOrder;
import com.datasqrl.plan.table.TableType;
import com.datasqrl.plan.table.TimestampHolder;
import com.datasqrl.plan.table.TopNConstraint;
import com.datasqrl.plan.table.VirtualRelationalTable;
import com.datasqrl.plan.table.VirtualRelationalTable.Child;
import com.datasqrl.plan.table.VirtualRelationalTable.Root;
import com.datasqrl.plan.util.ContinuousIndexMap;
import com.datasqrl.plan.util.ContinuousIndexMap.Builder;
import com.datasqrl.plan.util.TimeTumbleFunctionCall;
import com.datasqrl.plan.util.TimestampAnalysis;
import com.datasqrl.plan.util.TimestampAnalysis.MaxTimestamp;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.plan.util.IndexMap;
import com.datasqrl.util.SqrlRexUtil;
import com.datasqrl.plan.util.TimePredicate;
import com.datasqrl.plan.util.TimePredicateAnalyzer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.SqlKind;
import com.datasqrl.model.StreamType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;

@Value
public class SQRLLogicalPlanRewriter extends AbstractSqrlRelShuttle<AnnotatedLP> {

  public static final long UPPER_BOUND_INTERVAL_MS = 999L * 365L * 24L * 3600L; //999 years
  public static final String UNION_TIMESTAMP_COLUMN_NAME = "_timestamp";
  public static final String DEFAULT_PRIMARY_KEY_COLUMN_NAME = "_pk";

  RelBuilder relBuilder;
  SqrlRexUtil rexUtil;
  Config config;
  ErrorCollector errors;

  ExecutionAnalysis exec;

  SQRLLogicalPlanRewriter(@NonNull RelBuilder relBuilder, @NonNull ExecutionAnalysis exec,
      @NonNull ErrorCollector errors, @NonNull Config config) {
    this.relBuilder = relBuilder;
    this.rexUtil = new SqrlRexUtil(relBuilder.getTypeFactory());
    this.config = config;
    this.errors = errors;
    this.exec = exec;
  }

  private RelBuilder makeRelBuilder() {
    RelBuilder rel = relBuilder.transform(config -> config.withPruneInputOfAggregate(false));
    return rel;
  }

  @Override
  protected RelNode setRelHolder(AnnotatedLP relHolder) {
    Preconditions.checkArgument(relHolder.timestamp.hasCandidates(),"Invalid timestamp");
    if (!exec.supports(EngineCapability.PULLUP_OPTIMIZATION)) {
      //Inline all pullups
      RelBuilder relB = makeRelBuilder();
      relHolder = relHolder.inlineNowFilter(relB, exec).inlineTopN(relB, exec);
    }
    super.setRelHolder(relHolder);
    this.relHolder = relHolder;
    return relHolder.getRelNode();
  }

  @Override
  public RelNode visit(TableScan tableScan) {
    return setRelHolder(processTableScan(tableScan));
  }

  public AnnotatedLP processTableScan(TableScan tableScan) {
    //The base scan tables for all SQRL queries are VirtualRelationalTable
    VirtualRelationalTable vtable = tableScan.getTable().unwrap(VirtualRelationalTable.class);
    Preconditions.checkArgument(vtable != null);

    //Fields might have been added to vtable, so we need to trim the selects
    int numColumns = tableScan.getRowType().getFieldCount();

    ScriptRelationalTable queryTable = vtable.getRoot().getBase();
    config.getSourceTableConsumer().accept(queryTable);

    Optional<Integer> numRootPks = Optional.of(vtable.getRoot().getNumPrimaryKeys());

    if (exec.supports(EngineCapability.DENORMALIZE)) {
      //We have to append a timestamp to nested tables that are being normalized when
      //all columns are selected
      boolean appendTimestamp = !vtable.isRoot() && numColumns == vtable.getNumColumns()
          && config.isAddTimestamp2NormalizedChildTable();
      return denormalizeTable(queryTable, vtable, numColumns, numRootPks, appendTimestamp);
    } else {
      if (vtable.isRoot()) {
        return createAnnotatedRootTableScan(queryTable, numColumns,
            (VirtualRelationalTable.Root)vtable, numRootPks);
      } else {
        return createAnnotatedChildTableScan(queryTable, numColumns, vtable, numRootPks);
      }
    }
  }

  private AnnotatedLP denormalizeTable(ScriptRelationalTable queryTable,
      VirtualRelationalTable vtable, int numColumns, Optional<Integer> numRootPks,
      boolean appendTimestamp) {
    //Shred the virtual table all the way to root:
    //First, we prepare all the data structures
    Builder indexMap = ContinuousIndexMap.builder(numColumns);
    ContinuousIndexMap.Builder primaryKey = ContinuousIndexMap.builder(vtable.getNumPrimaryKeys());
    List<JoinTable> joinTables = new ArrayList<>();
    //Now, we shred
    RelNode relNode = shredTable(vtable, primaryKey, indexMap, joinTables, true).build();

    //Set timestamp
    TimestampHolder.Derived timestamp = queryTable.getTimestamp().getDerived();
    if (appendTimestamp) {
      indexMap.add(timestamp.getTimestampCandidate().getIndex());
    }

    int targetLength = relNode.getRowType().getFieldCount();
    AnnotatedLP result = new AnnotatedLP(relNode, queryTable.getType(),
        primaryKey.build(targetLength),
        timestamp,
        indexMap.build(targetLength), joinTables, numRootPks,
        queryTable.getPullups().getNowFilter(), queryTable.getPullups().getTopN(),
        queryTable.getPullups().getSort(), List.of());
    return result;
  }

  private AnnotatedLP createAnnotatedRootTableScan(ScriptRelationalTable queryTable,
      int numColumns, VirtualRelationalTable.Root vtable, Optional<Integer> numRootPks) {
    int targetLength = vtable.getNumColumns();
    PullupOperator.Container pullups = queryTable.getPullups();

    RelBuilder builder = makeRelBuilder();
    builder.scan(vtable.getNameId());

    TopNConstraint topN = pullups.getTopN();
    if (exec.isMaterialize(queryTable) && topN.isDeduplication()) {
      // We can drop topN since that gets enforced by writing to DB with primary key
      topN = TopNConstraint.EMPTY;
    }
    IndexMap query2virtualTable = vtable.mapQueryTable();
    return new AnnotatedLP(builder.build(), queryTable.getType(),
        ContinuousIndexMap.identity(vtable.getNumPrimaryKeys(), targetLength),
        queryTable.getTimestamp().getDerived().remapIndexes(query2virtualTable),
        ContinuousIndexMap.identity(numColumns, targetLength),
         List.of(JoinTable.ofRoot(vtable, NormType.NORMALIZED)), numRootPks,
        pullups.getNowFilter().remap(query2virtualTable), topN,
        pullups.getSort().remap(query2virtualTable), List.of());
  }

  private AnnotatedLP createAnnotatedChildTableScan(ScriptRelationalTable queryTable,
      int numColumns, VirtualRelationalTable vtable, Optional<Integer> numRootPks) {
    int targetLength = vtable.getNumColumns();
    PullupOperator.Container pullups = queryTable.getPullups();
    Preconditions.checkArgument(pullups.getTopN().isEmpty() && pullups.getNowFilter().isEmpty());

    //For this stage, data is normalized and we need to re-scan the VirtualRelationalTable
    //because it now has a timestamp at the end
    RelBuilder builder = makeRelBuilder();
    builder.scan(vtable.getNameId()); //table now has a timestamp column at the end
    Preconditions.checkArgument(targetLength==builder.peek().getRowType().getFieldCount());
    Preconditions.checkArgument(queryTable.getTimestamp().hasFixedTimestamp());
    int timestampIdx = targetLength-1;
    IndexMap remapTimestamp = IndexMap.singleton(queryTable.getTimestamp()
        .getTimestampCandidate().getIndex(),timestampIdx);

    //ToDo: need to join with root table on timestamp to ensure we are filtering out outdated
    //versions from previous upserts
    JoinTable joinTable = new JoinTable(vtable, null, JoinRelType.INNER, 0, NormType.NORMALIZED);
    return new AnnotatedLP(builder.build(), queryTable.getType(),
        ContinuousIndexMap.identity(vtable.getNumPrimaryKeys(), targetLength),
        queryTable.getTimestamp().getDerived().remapIndexes(remapTimestamp),
        ContinuousIndexMap.identity(numColumns, targetLength),
         List.of(joinTable), numRootPks,
        NowFilter.EMPTY, TopNConstraint.EMPTY,
        SortOrder.EMPTY, List.of());
  }

  private RelBuilder shredTable(VirtualRelationalTable vtable,
      ContinuousIndexMap.Builder primaryKey,
      ContinuousIndexMap.Builder select, List<JoinTable> joinTables,
      boolean isLeaf) {
    Preconditions.checkArgument(joinTables.isEmpty());
    return shredTable(vtable, primaryKey, select, joinTables, null, JoinRelType.INNER, isLeaf);
  }

  private RelBuilder shredTable(VirtualRelationalTable vtable,
      ContinuousIndexMap.Builder primaryKey,
      List<JoinTable> joinTables, Pair<JoinTable, RelBuilder> startingBase,
      JoinRelType joinType) {
    Preconditions.checkArgument(joinTables.isEmpty());
    return shredTable(vtable, primaryKey, null, joinTables, startingBase, joinType, false);
  }

  @Value
  public static class ShredTableResult {
    RelBuilder builder;
    int offset;
    JoinTable joinTable;
  }

  private RelBuilder shredTable(VirtualRelationalTable vtable,
      ContinuousIndexMap.Builder primaryKey,
      ContinuousIndexMap.Builder select, List<JoinTable> joinTables,
      Pair<JoinTable, RelBuilder> startingBase, JoinRelType joinType, boolean isLeaf) {
    Preconditions.checkArgument(joinType == JoinRelType.INNER || joinType == JoinRelType.LEFT);

    if (startingBase != null && startingBase.getKey().getTable().equals(vtable)) {
      joinTables.add(startingBase.getKey());
      return startingBase.getValue();
    }

    ShredTableResult tableResult;
    if (vtable.isRoot()) {
      tableResult = shredRootTable((VirtualRelationalTable.Root) vtable);
    } else {
      tableResult = shredChildTable((VirtualRelationalTable.Child) vtable, primaryKey, select, joinTables, startingBase,
          joinType);
    }
    int offset = tableResult.getOffset();
    for (int i = 0; i < vtable.getNumLocalPks(); i++) {
      primaryKey.add(offset + i);
      if (!isLeaf && startingBase == null) {
        select.add(offset + i);
      }
    }
    addAdditionalColumns(vtable, tableResult.getBuilder(), tableResult.getJoinTable());
    joinTables.add(tableResult.getJoinTable());
    constructLeafIndexMap(vtable, select, isLeaf, startingBase, tableResult.getOffset());

    return tableResult.getBuilder();
  }

  private ShredTableResult shredRootTable(Root root) {
    RelBuilder builder = makeRelBuilder();
    builder.scan(root.getBase().getNameId());
    JoinTable joinTable = JoinTable.ofRoot(root, NormType.DENORMALIZED);
    return new ShredTableResult(builder, 0, joinTable);
  }

  private ShredTableResult shredChildTable(Child child, ContinuousIndexMap.Builder primaryKey,
      ContinuousIndexMap.Builder select, List<JoinTable> joinTables,
      Pair<JoinTable, RelBuilder> startingBase, JoinRelType joinType) {
    RelBuilder builder = shredTable(child.getParent(), primaryKey, select, joinTables, startingBase,
        joinType, false);
    JoinTable parentJoinTable = Iterables.getLast(joinTables);
    int indexOfShredField = parentJoinTable.getOffset() + child.getShredIndex();
    CorrelationId id = builder.getCluster().createCorrel();
    RelDataType base = builder.peek().getRowType();
    int offset = base.getFieldCount();
    JoinTable joinTable = new JoinTable(child, parentJoinTable, joinType, offset, NormType.DENORMALIZED);

    builder
        .values(List.of(List.of(rexUtil.getBuilder().makeExactLiteral(BigDecimal.ZERO))),
            new RelRecordType(List.of(new RelDataTypeFieldImpl(
                "ZERO",
                0,
                builder.getTypeFactory().createSqlType(SqlTypeName.INTEGER)))))
        .project(
            List.of(rexUtil.getBuilder()
                .makeFieldAccess(
                    rexUtil.getBuilder().makeCorrel(base, id),
                    indexOfShredField)))
        .uncollect(List.of(), false)
        .correlate(joinType, id, RexInputRef.of(indexOfShredField, base));
    return new ShredTableResult(builder, offset, joinTable);
  }

  private void addAdditionalColumns(VirtualRelationalTable vtable, RelBuilder builder, JoinTable joinTable) {
    //Add additional columns
    JoinTable.Path path = JoinTable.Path.of(joinTable);
    for (AddedColumn column : vtable.getAddedColumns()) {
      //How do columns impact materialization preference (e.g. contain function that cannot be computed in DB) if they might get projected out again
      AddedColumn.Simple simpleCol = (AddedColumn.Simple) column;
      RexNode added = simpleCol.getExpression(path.mapLeafTable());
      rexUtil.appendColumn(builder, added, simpleCol.getNameId());
    }
  }

  public void constructLeafIndexMap(VirtualRelationalTable vtable,
      ContinuousIndexMap.Builder select, boolean isLeaf, Pair<JoinTable, RelBuilder> startingBase,
      int offset) {
    //Construct indexMap if this shred table is the leaf (i.e. the one we are expanding)
    if (isLeaf && startingBase == null) {
      //All non-nested fields are part of the virtual table query row type
      List<RelDataTypeField> queryRowType = vtable.getQueryRowType().getFieldList();
      for (int i = 0; i < queryRowType.size() && select.remaining()>0; i++) {
        RelDataTypeField field = queryRowType.get(i);
        if (!CalciteUtil.isNestedTable(field.getType())) {
          select.add(offset + i);
        }
      }
    }
  }

  private static final SqrlRexUtil.RexFinder FIND_NOW = SqrlRexUtil.findFunction(
      NOW);

  @Override
  public RelNode visit(LogicalFilter logicalFilter) {
    AnnotatedLP input = getRelHolder(logicalFilter.getInput().accept(this));
    input = input.inlineTopN(makeRelBuilder(), exec); //Filtering doesn't preserve deduplication
    RexNode condition = logicalFilter.getCondition();
    condition = input.select.map(condition);
    TimestampHolder.Derived timestamp = input.timestamp;
    NowFilter nowFilter = input.nowFilter;

    //Check if it has a now() predicate and pull out or throw an exception if malformed
    RelBuilder relBuilder = makeRelBuilder();
    relBuilder.push(input.relNode);
    List<TimePredicate> timeFunctions = new ArrayList<>();
    List<RexNode> conjunctions = null;
    if (FIND_NOW.foundIn(condition)) {
      conjunctions = rexUtil.getConjunctions(condition);
      Iterator<RexNode> iter = conjunctions.iterator();
      while (iter.hasNext()) {
        RexNode conj = iter.next();
        if (FIND_NOW.foundIn(conj)) {
          Optional<TimePredicate> tp = TimePredicateAnalyzer.INSTANCE.extractTimePredicate(conj,
                  rexUtil.getBuilder(),
                  timestamp.isCandidatePredicate())
              .filter(TimePredicate::hasTimestampFunction);
          if (tp.isPresent() && tp.get().isNowPredicate()) {
            timeFunctions.add(tp.get());
            iter.remove();
          } else {
                        /*Filter is not on a timestamp or not parsable, we leave it as is. In the future we can consider
                        pulling up now-filters on non-timestamp columns since we need to push those into the database
                        anyways. However, we cannot do much else with those (e.g. convert to time-windows or TTL) since
                        they are not based on the timeline.
                         */
            //TODO: issue warning
          }
        }
      }
    }
    if (!timeFunctions.isEmpty()) {
      Optional<NowFilter> combinedFilter = NowFilter.of(timeFunctions);
      Optional<NowFilter> resultFilter = combinedFilter.flatMap(nowFilter::merge);
      errors.checkFatal(resultFilter.isPresent(),"Found one or multiple now-filters that cannot be satisfied.");
      nowFilter = resultFilter.get();
      int timestampIdx = nowFilter.getTimestampIndex();
      timestamp = timestamp.getCandidateByIndex(timestampIdx).fixAsTimestamp();
      //Add as static time filter (to push down to source)
      NowFilter localNowFilter = combinedFilter.get();
      //TODO: add back in, push down to push into source, then remove
      //localNowFilter.addFilterTo(relBuilder,true);
    } else {
      conjunctions = List.of(condition);
    }
    relBuilder.filter(conjunctions);
    exec.requireRex(conjunctions);
    return setRelHolder(input.copy().relNode(relBuilder.build()).timestamp(timestamp)
        .nowFilter(nowFilter).build());
  }

  @Override
  public RelNode visit(LogicalProject logicalProject) {
    AnnotatedLP rawInput = getRelHolder(logicalProject.getInput().accept(this));

    ContinuousIndexMap trivialMap = getTrivialMapping(logicalProject, rawInput.select);
    if (trivialMap != null) {
      //Check if this is a topN constraint
      Optional<TopNHint> topNHintOpt = SqrlHint.fromRel(logicalProject, TopNHint.CONSTRUCTOR);
      if (topNHintOpt.isPresent()) {
        TopNHint topNHint = topNHintOpt.get();

        RelNode base = logicalProject.getInput();
        RelCollation collation = RelCollations.EMPTY;
        Optional<Integer> limit = Optional.empty();
        if (base instanceof LogicalSort) {
          LogicalSort nestedSort = (LogicalSort) base;
          base = nestedSort.getInput();
          collation = nestedSort.getCollation();
          limit = getLimit(nestedSort.fetch);
        }

        AnnotatedLP baseInput = getRelHolder(base.accept(this));
        baseInput = baseInput.inlineNowFilter(makeRelBuilder(), exec).inlineTopN(makeRelBuilder(), exec);
        int targetLength = baseInput.getFieldLength();

        collation = baseInput.select.map(collation);
        if (collation.getFieldCollations().isEmpty()) {
          collation = baseInput.sort.getCollation();
        }
        List<Integer> partition = topNHint.getPartition().stream().map(baseInput.select::map)
            .collect(Collectors.toList());

        ContinuousIndexMap pk = baseInput.primaryKey;
        ContinuousIndexMap select = trivialMap;
        TimestampHolder.Derived timestamp = baseInput.timestamp;
        boolean isDistinct = false;
        if (topNHint.getType() == TopNHint.Type.SELECT_DISTINCT) {
          List<Integer> distincts = SqrlRexUtil.combineIndexes(partition,
              trivialMap.targetsAsList());
          pk = ContinuousIndexMap.builder(distincts.size()).addAll(distincts).build(targetLength);
          if (partition.isEmpty()) {
            //If there is no partition, we can ignore the sort order plus limit and turn this into a simple deduplication
            partition = pk.targetsAsList();
            if (timestamp.hasCandidates()) {
              timestamp = timestamp.getBestCandidate().fixAsTimestamp();
              collation = LPConverterUtil.getTimestampCollation(timestamp.getTimestampCandidate());
            } else {
              collation = RelCollations.of(distincts.get(0));
            }
            limit = Optional.of(1);
          } else {
            isDistinct = true;
          }
        } else if (topNHint.getType() == TopNHint.Type.DISTINCT_ON) {
          Preconditions.checkArgument(!partition.isEmpty());
          Preconditions.checkArgument(!collation.getFieldCollations().isEmpty());
          errors.checkFatal(LPConverterUtil.getTimestampOrderIndex(collation, timestamp).isPresent(),
              DISTINCT_ON_TIMESTAMP, "Not a valid timestamp order in ORDER BY clause: %s",
              rexUtil.getCollationName(collation, baseInput.relNode));
          errors.checkFatal(baseInput.type.isStream(), WRONG_TABLE_TYPE,
              "DISTINCT ON statements require stream table as input");

          pk = ContinuousIndexMap.builder(partition.size()).addAll(partition).build(targetLength);
          select = ContinuousIndexMap.identity(targetLength, targetLength); //Select everything
          //Extract timestamp from collation
          TimestampHolder.Derived.Candidate candidate = LPConverterUtil.getTimestampOrderIndex(
              collation, timestamp).get();
          collation = LPConverterUtil.getTimestampCollation(candidate);
          timestamp = candidate.fixAsTimestamp();
          limit = Optional.of(1); //distinct on has implicit limit of 1
        } else if (topNHint.getType() == TopNHint.Type.TOP_N) {
          //Prepend partition to primary key
          List<Integer> pkIdx = SqrlRexUtil.combineIndexes(partition, pk.targetsAsList());
          pk = ContinuousIndexMap.builder(pkIdx.size()).addAll(pkIdx).build(targetLength);
        }

        TopNConstraint topN = new TopNConstraint(partition, isDistinct, collation, limit,
            LPConverterUtil.getTimestampOrderIndex(collation, timestamp).isPresent());
        TableType resultType = TableType.STATE;
        if (baseInput.type.isStream() && topN.isDeduplication()) resultType = TableType.DEDUP_STREAM;
        return setRelHolder(baseInput.copy().type(resultType)
            .primaryKey(pk).select(select).timestamp(timestamp)
            .joinTables(null).topN(topN).sort(SortOrder.EMPTY).build());
      } else {
        //If it's a trivial project, we remove it and only update the indexMap. This is needed to eliminate self-joins
        return setRelHolder(rawInput.copy().select(trivialMap).build());
      }
    }
    AnnotatedLP input = rawInput.inlineTopN(makeRelBuilder(), exec);
    //Update index mappings
    List<RexNode> updatedProjects = new ArrayList<>();
    List<String> updatedNames = new ArrayList<>();
    //We only keep track of the first mapped project and consider it to be the "preserving one" for primary keys and timestamps
    Map<Integer, Integer> mappedProjects = new HashMap<>();
    List<TimestampHolder.Derived.Candidate> timeCandidates = new ArrayList<>();
    NowFilter nowFilter = NowFilter.EMPTY;
    for (Ord<RexNode> exp : Ord.<RexNode>zip(logicalProject.getProjects())) {
      RexNode mapRex = input.select.map(exp.e);
      updatedProjects.add(exp.i, mapRex);
      updatedNames.add(exp.i, logicalProject.getRowType().getFieldNames().get(exp.i));
      int originalIndex = -1;
      if (mapRex instanceof RexInputRef) { //Direct mapping
        originalIndex = (((RexInputRef) mapRex)).getIndex();
      } else { //Check for preserved timestamps
        Optional<TimestampHolder.Derived.Candidate> preservedCandidate = TimestampAnalysis.getPreservedTimestamp(
            mapRex, input.timestamp);
        if (preservedCandidate.isPresent()) {
          originalIndex = preservedCandidate.get().getIndex();
          timeCandidates.add(preservedCandidate.get().withIndex(exp.i));
          //See if we can preserve the now-filter as well or need to inline it
          if (!input.nowFilter.isEmpty() && input.nowFilter.getTimestampIndex() == originalIndex) {
            Optional<TimeTumbleFunctionCall> bucketFct = TimeTumbleFunctionCall.from(mapRex,
                rexUtil.getBuilder());
            if (bucketFct.isPresent()) {
              long intervalExpansion = bucketFct.get().getSpecification().getWindowWidthMillis();
              nowFilter = input.nowFilter.map(tp -> new TimePredicate(tp.getSmallerIndex(),
                  exp.i, tp.getComparison(), tp.getIntervalLength() + intervalExpansion));
            } else {
              input = input.inlineNowFilter(makeRelBuilder(), exec);
            }
          }
        }
      }
      if (originalIndex >= 0) {
        if (mappedProjects.putIfAbsent(originalIndex, exp.i) != null) {
          Optional<TimestampHolder.Derived.Candidate> originalCand = input.timestamp.getOptCandidateByIndex(
              originalIndex);
          originalCand.ifPresent(c -> timeCandidates.add(c.withIndex(exp.i)));
          //We are ignoring this mapping because the prior one takes precedence, let's see if we should warn the user
          if (input.primaryKey.containsTarget(originalIndex)) {
            //TODO: issue a warning to alert the user that this mapping is not considered part of primary key
            System.out.println("WARNING: mapping primary key multiple times");
          }
        }
      }
    }
    //Make sure we pull the primary keys and timestamp candidates through (i.e. append those to the projects
    //if not already present)
    ContinuousIndexMap.Builder primaryKey = ContinuousIndexMap.builder(
        input.primaryKey.getSourceLength());
    for (IndexMap.Pair p : input.primaryKey.getMapping()) {
      Integer target = mappedProjects.get(p.getTarget());
      if (target == null) {
        //Need to add it
        target = updatedProjects.size();
        updatedProjects.add(target, RexInputRef.of(p.getTarget(), input.relNode.getRowType()));
        updatedNames.add(null);
        mappedProjects.put(p.getTarget(), target);
      }
      primaryKey.add(target);
    }
    for (TimestampHolder.Derived.Candidate candidate : input.timestamp.getCandidates()) {
      //Check if candidate is already mapped through timestamp preserving function
      if (timeCandidates.contains(candidate)) {
        continue;
      }
      Integer target = mappedProjects.get(candidate.getIndex());
      if (target == null) {
        //Need to add candidate
        target = updatedProjects.size();
        updatedProjects.add(target,
            RexInputRef.of(candidate.getIndex(), input.relNode.getRowType()));
        updatedNames.add(null);
        mappedProjects.put(candidate.getIndex(), target);
      }
      //Update now-filter if it matches candidate
      if (!input.nowFilter.isEmpty()
          && input.nowFilter.getTimestampIndex() == candidate.getIndex()) {
        nowFilter = input.nowFilter.remap(IndexMap.singleton(candidate.getIndex(), target));
      }
      timeCandidates.add(candidate.withIndex(target));
    }
    TimestampHolder.Derived timestamp = input.timestamp.restrictTo(timeCandidates);
    //NowFilter must have been preserved
    assert !nowFilter.isEmpty() || input.nowFilter.isEmpty();

    //TODO: preserve sort
    List<RelFieldCollation> collations = new ArrayList<>(
        input.sort.getCollation().getFieldCollations());
    for (int i = 0; i < collations.size(); i++) {
      RelFieldCollation fieldcol = collations.get(i);
      Integer target = mappedProjects.get(fieldcol.getFieldIndex());
      if (target == null) {
        //Need to add candidate
        target = updatedProjects.size();
        updatedProjects.add(target,
            RexInputRef.of(fieldcol.getFieldIndex(), input.relNode.getRowType()));
        updatedNames.add(null);
        mappedProjects.put(fieldcol.getFieldIndex(), target);
      }
      collations.set(i, fieldcol.withFieldIndex(target));
    }
    SortOrder sort = new SortOrder(RelCollations.of(collations));

    //Build new project
    RelBuilder relB = makeRelBuilder();
    relB.push(input.relNode);
    relB.project(updatedProjects, updatedNames);
    RelNode newProject = relB.build();
    int fieldCount = updatedProjects.size();
    exec.requireRex(updatedProjects);
    return setRelHolder(AnnotatedLP.build(newProject, input.type, primaryKey.build(fieldCount),
            timestamp, ContinuousIndexMap.identity(logicalProject.getProjects().size(), fieldCount),
            input)
        .numRootPks(input.numRootPks).nowFilter(nowFilter).sort(sort).build());
  }

  private ContinuousIndexMap getTrivialMapping(LogicalProject project, ContinuousIndexMap baseMap) {
    ContinuousIndexMap.Builder b = ContinuousIndexMap.builder(project.getProjects().size());
    for (RexNode rex : project.getProjects()) {
      if (!(rex instanceof RexInputRef)) {
        return null;
      }
      b.add(baseMap.map((((RexInputRef) rex)).getIndex()));
    }
    return b.build();
  }

  @Override
  public RelNode visit(LogicalJoin logicalJoin) {
    AnnotatedLP leftIn = getRelHolder(logicalJoin.getLeft().accept(this));
    AnnotatedLP rightIn = getRelHolder(logicalJoin.getRight().accept(this));

    AnnotatedLP leftInput = leftIn.inlineTopN(makeRelBuilder(), exec);
    AnnotatedLP rightInput = rightIn.inlineTopN(makeRelBuilder(), exec);
    JoinRelType joinType = logicalJoin.getJoinType();

    final int leftSideMaxIdx = leftInput.getFieldLength();
    ContinuousIndexMap joinedIndexMap = leftInput.select.join(rightInput.select, leftSideMaxIdx);
    RexNode condition = joinedIndexMap.map(logicalJoin.getCondition());
    exec.requireRex(condition);
    //TODO: pull now() conditions up as a nowFilter and move nested now filters through
    errors.checkFatal(!FIND_NOW.foundIn(condition),
        "now() is not allowed in join conditions");
    SqrlRexUtil.EqualityComparisonDecomposition eqDecomp = rexUtil.decomposeEqualityComparison(
        condition);

    //Identify if this is an identical self-join for a nested tree
    boolean hasTransformativePullups =
        !leftIn.topN.isEmpty() || !rightIn.topN.isEmpty();
    if ((joinType == JoinRelType.DEFAULT || joinType == JoinRelType.INNER || joinType == JoinRelType.LEFT)
        && JoinTable.compatible(leftInput.joinTables, rightInput.joinTables)
        && !hasTransformativePullups
        && eqDecomp.getRemainingPredicates().isEmpty() && !eqDecomp.getEqualities().isEmpty()) {
      errors.checkFatal(JoinTable.getRoots(rightInput.joinTables).size() == 1,
          ErrorCode.NOT_YET_IMPLEMENTED,
          "Current assuming a single table on the right for self-join elimination.");

      //Determine if we can map the tables from both branches of the join onto each-other
      Map<JoinTable, JoinTable> right2left;
      if (exec.supports(EngineCapability.DENORMALIZE)) {
        right2left = JoinTable.joinTreeMap(leftInput.joinTables, leftSideMaxIdx,
            rightInput.joinTables, eqDecomp.getEqualities());
      } else {
        right2left = JoinTable.joinListMap(leftInput.joinTables, leftSideMaxIdx,
            rightInput.joinTables, eqDecomp.getEqualities());
      }
      if (!right2left.isEmpty()) {
        JoinTable rightLeaf = Iterables.getOnlyElement(JoinTable.getLeafs(rightInput.joinTables));
        RelBuilder relBuilder = makeRelBuilder().push(leftInput.getRelNode());
        ContinuousIndexMap newPk = leftInput.primaryKey;
        List<JoinTable> joinTables = new ArrayList<>(leftInput.joinTables);
        if (joinType == JoinRelType.DEFAULT) {
          joinType = JoinRelType.INNER;
        }
        if (!right2left.containsKey(rightLeaf)) {
          //Find closest ancestor that was mapped and shred from there
          List<JoinTable> ancestorPath = new ArrayList<>();
          int numAddedPks = 0;
          ancestorPath.add(rightLeaf);
          JoinTable ancestor = rightLeaf;
          while (!right2left.containsKey(ancestor)) {
            numAddedPks += ancestor.getNumLocalPk();
            ancestor = ancestor.parent;
            ancestorPath.add(ancestor);
          }
          Collections.reverse(
              ancestorPath); //To match the order of addedTables when shredding (i.e. from root to leaf)
          ContinuousIndexMap.Builder addedPk = ContinuousIndexMap.builder(newPk, numAddedPks);
          List<JoinTable> addedTables = new ArrayList<>();
          relBuilder = shredTable(rightLeaf.table, addedPk, addedTables,
              Pair.of(right2left.get(ancestor), relBuilder), joinType);
          newPk = addedPk.build(relBuilder.peek().getRowType().getFieldCount());
          Preconditions.checkArgument(ancestorPath.size() == addedTables.size());
          for (int i = 1; i < addedTables.size();
              i++) { //First table is the already mapped root ancestor
            joinTables.add(addedTables.get(i));
            right2left.put(ancestorPath.get(i), addedTables.get(i));
          }
        } else if (!right2left.get(rightLeaf).getTable().equals(rightLeaf.getTable())) {
          //When mapping normalized tables, the mapping might map siblings which we need to join
          Preconditions.checkArgument(!exec.supports(EngineCapability.DENORMALIZE));
          JoinTable leftTbl = right2left.get(rightLeaf);
          relBuilder.push(rightInput.getRelNode());
          relBuilder.join(joinType, condition);
          JoinTable rightTbl = rightLeaf.withOffset(leftSideMaxIdx);
          joinTables.add(rightTbl);
          right2left.put(rightLeaf,rightTbl);
          //Add any additional pks
          int rightNumPk = rightLeaf.getNumPkNormalized(), leftNumPk = leftTbl.getNumPkNormalized();
          if (rightNumPk > leftNumPk) {
            int deltaPk = rightNumPk - leftNumPk;
            ContinuousIndexMap.Builder addedPk = ContinuousIndexMap.builder(newPk, deltaPk);
            for (int i = 0; i < deltaPk; i++) {
              addedPk.add(rightTbl.getGlobalIndex(leftNumPk + i));
            }
            newPk = addedPk.build();
          }
         }
        RelNode relNode = relBuilder.build();
        //Update indexMap based on the mapping of join tables
        final List<JoinTable> rightTables = rightInput.joinTables;
        ContinuousIndexMap remapedRight = rightInput.select.remap(
            index -> {
              JoinTable jt = JoinTable.find(rightTables, index).get();
              return right2left.get(jt).getGlobalIndex(jt.getLocalIndex(index));
            });
        //Combine now-filters if they exist
        NowFilter resultNowFilter = leftInput.nowFilter;
        if (!rightInput.nowFilter.isEmpty()) {
          resultNowFilter = leftInput.nowFilter.merge(
                  rightInput.nowFilter.remap(IndexMap.singleton( //remap to use left timestamp
                      rightInput.timestamp.getTimestampCandidate().getIndex(),
                      leftInput.timestamp.getTimestampCandidate().getIndex())))
              .orElseGet(() -> {
                errors.fatal("Could not combine now-filters");
                return NowFilter.EMPTY;
              });
        }


        ContinuousIndexMap indexMap = leftInput.select.append(remapedRight);
        return setRelHolder(AnnotatedLP.build(relNode, leftInput.type, newPk, leftInput.timestamp,
                indexMap, leftInput)
            .numRootPks(leftInput.numRootPks).nowFilter(resultNowFilter)
            .joinTables(joinTables).build());
      }
    }

    //Detect temporal join
    if (joinType == JoinRelType.DEFAULT || joinType == JoinRelType.TEMPORAL
        || joinType == JoinRelType.LEFT) {
      if ((leftInput.type.isStream() && rightInput.type.isState()) ||
          (rightInput.type.isStream() && leftInput.type.isState()
              && joinType != JoinRelType.LEFT)) {
        //Make sure the stream is left and state is right
        if (rightInput.type.isStream()) {
          //Switch sides
          AnnotatedLP tmp = rightInput;
          rightInput = leftInput;
          leftInput = tmp;

          int tmpLeftSideMaxIdx = leftInput.getFieldLength();
          IndexMap leftRightFlip = idx -> idx < leftSideMaxIdx ? tmpLeftSideMaxIdx + idx
              : idx - leftSideMaxIdx;
          joinedIndexMap = joinedIndexMap.remap(leftRightFlip);
          condition = joinedIndexMap.map(logicalJoin.getCondition());
          eqDecomp = rexUtil.decomposeEqualityComparison(condition);
        }
        int newLeftSideMaxIdx = leftInput.getFieldLength();
        //Check for primary keys equalities on the state-side of the join
        Set<Integer> pkIndexes = rightInput.primaryKey.getMapping().stream()
            .map(p -> p.getTarget() + newLeftSideMaxIdx).collect(Collectors.toSet());
        Set<Integer> pkEqualities = eqDecomp.getEqualities().stream().map(p -> p.target)
            .collect(Collectors.toSet());
        if (pkIndexes.equals(pkEqualities) && eqDecomp.getRemainingPredicates().isEmpty() &&
            rightInput.nowFilter.isEmpty()) {
          RelBuilder relB = makeRelBuilder();
          relB.push(leftInput.relNode);
          RelNode state = rightInput.relNode;
          if (rightInput.type!=TableType.DEDUP_STREAM && !exec.supports(EngineCapability.TEMPORAL_JOIN_ON_STATE)) {
            //Need to convert to stream and add dedup to make this work
            AnnotatedLP state2stream = makeStream(rightInput, StreamType.UPDATE);
            //stream has the same order of fields
//            TopNConstraint dedup = TopNConstraint.makeDeduplication(pkAndSelect.pk.targetsAsList(),
//                timestamp.getTimestampCandidate().getIndex());
            errors.checkFatal(false, ErrorCode.NOT_YET_IMPLEMENTED,
                "Currently temporal joins are limited to states that are deduplicated streams.");
          }

          relB.push(state);
          Preconditions.checkArgument(rightInput.timestamp.hasFixedTimestamp());
          TimestampHolder.Derived joinTimestamp = leftInput.timestamp.getBestCandidate()
              .fixAsTimestamp();

          ContinuousIndexMap pk = ContinuousIndexMap.builder(leftInput.primaryKey, 0)
              .build();
          TemporalJoinHint hint = new TemporalJoinHint(
              joinTimestamp.getTimestampCandidate().getIndex(),
              rightInput.timestamp.getTimestampCandidate().getIndex(),
              rightInput.primaryKey.targetsAsArray());
          relB.join(joinType == JoinRelType.LEFT ? joinType : JoinRelType.INNER, condition);
          hint.addTo(relB);
          exec.require(EngineCapability.TEMPORAL_JOIN);
          return setRelHolder(AnnotatedLP.build(relB.build(), TableType.STREAM,
                  pk, joinTimestamp, joinedIndexMap,
                  List.of(leftInput, rightInput))
              .joinTables(leftInput.joinTables)
              .numRootPks(leftInput.numRootPks).nowFilter(leftInput.nowFilter).sort(leftInput.sort)
              .build());
        } else if (joinType == JoinRelType.TEMPORAL) {
          errors.fatal("Expected join condition to be equality condition on state's primary key: %s",
                  logicalJoin);
        }
      } else if (joinType == JoinRelType.TEMPORAL) {
        errors.fatal("Expect one side of the join to be stream and the other temporal state: %s",
                logicalJoin);
      }

    }

    //TODO: pull now-filters through interval join where possible
    final AnnotatedLP leftInputF = leftInput.inlineNowFilter(makeRelBuilder(), exec);
    final AnnotatedLP rightInputF = rightInput.inlineNowFilter(makeRelBuilder(), exec);

    RelBuilder relB = makeRelBuilder();
    relB.push(leftInputF.relNode);
    relB.push(rightInputF.relNode);
    Function<Integer, RexInputRef> idxResolver = idx -> {
      if (idx < leftSideMaxIdx) {
        return RexInputRef.of(idx, leftInputF.relNode.getRowType());
      } else {
        return new RexInputRef(idx,
            rightInputF.relNode.getRowType().getFieldList().get(idx - leftSideMaxIdx).getType());
      }
    };

    ContinuousIndexMap.Builder concatPkBuilder;
    if (joinType == JoinRelType.LEFT) {
      concatPkBuilder = ContinuousIndexMap.builder(leftInputF.primaryKey, 0);
    } else {
      concatPkBuilder = ContinuousIndexMap.builder(leftInputF.primaryKey,
          rightInputF.primaryKey.getSourceLength());
      concatPkBuilder.addAll(rightInputF.primaryKey.remap(idx -> idx + leftSideMaxIdx));
    }
    ContinuousIndexMap concatPk = concatPkBuilder.build();

    //combine sorts if present
    SortOrder joinedSort = leftInputF.sort.join(
        rightInputF.sort.remap(idx -> idx + leftSideMaxIdx));

    //Detect interval join
    if (joinType == JoinRelType.DEFAULT || joinType == JoinRelType.INNER
        || joinType == JoinRelType.INTERVAL || joinType == JoinRelType.LEFT) {
      if (leftInputF.type.isStream() && rightInputF.type.isStream()) {
        //Validate that the join condition includes time bounds on both sides
        List<RexNode> conjunctions = new ArrayList<>(rexUtil.getConjunctions(condition));
        Predicate<Integer> isTimestampColumn = idx -> idx < leftSideMaxIdx
            ? leftInputF.timestamp.isCandidate(idx) :
            rightInputF.timestamp.isCandidate(idx - leftSideMaxIdx);
        List<TimePredicate> timePredicates = conjunctions.stream().map(rex ->
                TimePredicateAnalyzer.INSTANCE.extractTimePredicate(rex, rexUtil.getBuilder(),
                    isTimestampColumn))
            .flatMap(tp -> tp.stream()).filter(tp -> !tp.hasTimestampFunction())
            //making sure predicate contains columns from both sides of the join
            .filter(tp -> (tp.getSmallerIndex() < leftSideMaxIdx) ^ (tp.getLargerIndex()
                < leftSideMaxIdx))
            .collect(Collectors.toList());
        Optional<Integer> numRootPks = Optional.empty();
        if (timePredicates.isEmpty() && leftInputF.numRootPks.flatMap(npk ->
            rightInputF.numRootPks.filter(npk2 -> npk2.equals(npk))).isPresent()) {
          //If both streams have same number of root primary keys, check if those are part of equality conditions
          List<IntPair> rootPkPairs = new ArrayList<>();
          for (int i = 0; i < leftInputF.numRootPks.get(); i++) {
            rootPkPairs.add(new IntPair(leftInputF.primaryKey.map(i),
                rightInputF.primaryKey.map(i) + leftSideMaxIdx));
          }
          if (eqDecomp.getEqualities().containsAll(rootPkPairs)) {
            //Change primary key to only include root pk once and equality time condition because timestamps must be equal
            TimePredicate eqCondition = new TimePredicate(
                rightInputF.timestamp.getBestCandidate().getIndex() + leftSideMaxIdx,
                leftInputF.timestamp.getBestCandidate().getIndex(), SqlKind.EQUALS, 0);
            timePredicates.add(eqCondition);
            conjunctions.add(eqCondition.createRexNode(rexUtil.getBuilder(), idxResolver, false));

            numRootPks = leftInputF.numRootPks;
            //remove root pk columns from right side when combining primary keys
            concatPkBuilder = ContinuousIndexMap.builder(leftInputF.primaryKey,
                rightInputF.primaryKey.getSourceLength() - numRootPks.get());
            List<Integer> rightPks = rightInputF.primaryKey.targetsAsList();
            concatPkBuilder.addAll(rightPks.subList(numRootPks.get(), rightPks.size()).stream()
                .map(idx -> idx + leftSideMaxIdx).collect(Collectors.toList()));
            concatPk = concatPkBuilder.build();

          }
        }
        if (!timePredicates.isEmpty()) {
          Set<Integer> timestampIndexes = timePredicates.stream()
              .flatMap(tp -> tp.getIndexes().stream()).collect(Collectors.toSet());
          errors.checkFatal(timestampIndexes.size() == 2, WRONG_INTERVAL_JOIN,
              "Invalid interval condition - more than 2 timestamp columns: %s", condition);
          errors.checkFatal(timePredicates.stream()
                  .filter(TimePredicate::isUpperBound).count() == 1, WRONG_INTERVAL_JOIN,
              "Expected exactly one upper bound time predicate, but got: %s", condition);
          int upperBoundTimestampIndex = timePredicates.stream().filter(TimePredicate::isUpperBound)
              .findFirst().get().getLargerIndex();
          TimestampHolder.Derived joinTimestamp = null;
          //Lock in timestamp candidates for both sides and propagate timestamp
          for (int tidx : timestampIndexes) {
            TimestampHolder.Derived newTimestamp = apply2JoinSide(tidx, leftSideMaxIdx, leftInputF,
                rightInputF,
                (prel, idx) -> prel.timestamp.getCandidateByIndex(idx).withIndex(tidx)
                    .fixAsTimestamp());
            if (joinType == JoinRelType.LEFT) {
              if (tidx < leftSideMaxIdx) {
                joinTimestamp = newTimestamp;
              }
            } else if (tidx == upperBoundTimestampIndex) {
              joinTimestamp = newTimestamp;
            }
          }
          assert joinTimestamp != null;

          if (timePredicates.size() == 1 && !timePredicates.get(0).isEquality()) {
            //We only have an upper bound, add (very loose) bound in other direction - Flink requires this
            conjunctions.add(Iterables.getOnlyElement(timePredicates)
                .inverseWithInterval(UPPER_BOUND_INTERVAL_MS).createRexNode(rexUtil.getBuilder(),
                    idxResolver, false));
          }
          condition = RexUtil.composeConjunction(rexUtil.getBuilder(), conjunctions);
          relB.join(joinType == JoinRelType.LEFT ? joinType : JoinRelType.INNER,
              condition); //Can treat as "standard" inner join since no modification is necessary in physical plan
          SqrlHintStrategyTable.INTERVAL_JOIN.addTo(relB);
          return setRelHolder(AnnotatedLP.build(relB.build(), TableType.STREAM,
              concatPk, joinTimestamp, joinedIndexMap,
              List.of(leftInputF, rightInputF)).numRootPks(numRootPks).sort(joinedSort).build());
        } else if (joinType == JoinRelType.INTERVAL) {
          errors.fatal("Interval joins require time bounds in the join condition: " + logicalJoin);
        }
      } else if (joinType == JoinRelType.INTERVAL) {
        errors.fatal(
            "Interval joins are only supported between two streams: " + logicalJoin);
      }
    }

    //If we don't detect a special time-based join, a DEFAULT join is an INNER join
    if (joinType == JoinRelType.DEFAULT) {
      joinType = JoinRelType.INNER;
    }

    errors.checkFatal(joinType == JoinRelType.INNER || joinType == JoinRelType.LEFT,
        NOT_YET_IMPLEMENTED,"Unsupported join type: %s", logicalJoin);
    //Default inner join creates a state table
    relB.join(joinType, condition);
    new JoinCostHint(leftInputF.type, rightInputF.type, eqDecomp.getEqualities().size()).addTo(
        relB);

    //Determine timestamps for each side and add the max of those two as the resulting timestamp
    TimestampHolder.Derived.Candidate leftBest = leftInputF.timestamp.getBestCandidate();
    TimestampHolder.Derived.Candidate rightBest = rightInputF.timestamp.getBestCandidate();
    //New timestamp column is added at the end
    TimestampHolder.Derived resultTimestamp = leftBest.withIndex(relB.peek().getRowType().getFieldCount())
        .fixAsTimestamp();
    rightBest.fixAsTimestamp();
    RexNode maxTimestamp = rexUtil.maxOfTwoColumnsNotNull(leftBest.getIndex(),
        rightBest.getIndex()+leftSideMaxIdx, relB.peek());
    rexUtil.appendColumn(relB, maxTimestamp, ReservedName.SYSTEM_TIMESTAMP.getCanonical());

    return setRelHolder(AnnotatedLP.build(relB.build(), TableType.STATE,
        concatPk, resultTimestamp, joinedIndexMap,
        List.of(leftInputF, rightInputF)).sort(joinedSort).build());
  }

  private static <T, R> R apply2JoinSide(int joinIndex, int leftSideMaxIdx, T left, T right,
      BiFunction<T, Integer, R> function) {
    int idx;
    if (joinIndex >= leftSideMaxIdx) {
      idx = joinIndex - leftSideMaxIdx;
      return function.apply(right, idx);
    } else {
      idx = joinIndex;
      return function.apply(left, idx);
    }
  }

  @Override
  public RelNode visit(LogicalUnion logicalUnion) {
    errors.checkFatal(logicalUnion.all, NOT_YET_IMPLEMENTED,
        "Currently, only UNION ALL is supported. Combine with SELECT DISTINCT for UNION");

    List<AnnotatedLP> inputs = logicalUnion.getInputs().stream()
        .map(in -> getRelHolder(in.accept(this)).inlineTopN(makeRelBuilder(), exec))
        .map(meta -> meta.copy().sort(SortOrder.EMPTY)
            .build()) //We ignore the sorts of the inputs (if any) since they are streams and we union them the default sort is timestamp
        .map(meta -> meta.postProcess(
            makeRelBuilder(), null, exec)) //The post-process makes sure the input relations are aligned (pk,selects,timestamps)
        .collect(Collectors.toList());
    Preconditions.checkArgument(inputs.size() > 0);

    //In the following, we can assume that the input relations are aligned because we post-processed them
    RelBuilder relBuilder = makeRelBuilder();
    ContinuousIndexMap pk = inputs.get(0).primaryKey;
    ContinuousIndexMap select = inputs.get(0).select;
    Optional<Integer> numRootPks = inputs.get(0).numRootPks;
    int maxSelectIdx = Collections.max(select.targetsAsList()) + 1;
    List<Integer> selectIndexes = SqrlRexUtil.combineIndexes(pk.targetsAsList(),
        select.targetsAsList());
    List<String> selectNames = Collections.nCopies(maxSelectIdx, null);
    assert maxSelectIdx == selectIndexes.size()
        && ContiguousSet.closedOpen(0, maxSelectIdx).asList().equals(selectIndexes)
        : maxSelectIdx + " vs " + selectIndexes;

        /* Timestamp determination works as follows: First, we collect all candidates that are part of the selected indexes
          and are identical across all inputs. If this set is non-empty, it becomes the new timestamp. Otherwise, we pick
          the best timestamp candidate for each input, fix it, and append it as timestamp.
         */
    Set<Integer> timestampIndexes = inputs.get(0).timestamp.getCandidateIndexes().stream()
        .filter(idx -> idx < maxSelectIdx).collect(Collectors.toSet());
    for (AnnotatedLP input : inputs) {
      errors.checkFatal(input.type.isStream(), NOT_YET_IMPLEMENTED,
          "Only stream tables can currently be unioned. Union tables before converting them to state.");
      //Validate primary key and selects line up between the streams
      errors.checkFatal(pk.equals(input.primaryKey),
          "The tables in the union have different primary keys. UNION requires uniform primary keys.");
      errors.checkFatal(select.equals(input.select),
          "Input streams select different columns");
      timestampIndexes.retainAll(input.timestamp.getCandidateIndexes());
      numRootPks = numRootPks.flatMap(npk -> input.numRootPks.filter(npk2 -> npk.equals(npk2)));
    }

    TimestampHolder.Derived unionTimestamp = TimestampHolder.Derived.NONE;
    for (AnnotatedLP input : inputs) {
      TimestampHolder.Derived localTimestamp;
      List<Integer> localSelectIndexes = new ArrayList<>(selectIndexes);
      List<String> localSelectNames = new ArrayList<>(selectNames);
      if (timestampIndexes.isEmpty()) { //Pick best and append
        TimestampHolder.Derived.Candidate bestCand = input.timestamp.getBestCandidate();
        localSelectIndexes.add(bestCand.getIndex());
        localSelectNames.add(UNION_TIMESTAMP_COLUMN_NAME);
        localTimestamp = bestCand.withIndex(maxSelectIdx).fixAsTimestamp();
      } else {
        localTimestamp = input.timestamp.restrictTo(input.timestamp.getCandidates().stream()
            .filter(c -> timestampIndexes.contains(c.getIndex())).collect(Collectors.toList()));
      }

      relBuilder.push(input.relNode);
      CalciteUtil.addProjection(relBuilder, localSelectIndexes, localSelectNames);
      unionTimestamp = unionTimestamp.union(localTimestamp);
    }
    Preconditions.checkArgument(unionTimestamp.hasCandidates());
    relBuilder.union(true, inputs.size());
    return setRelHolder(
        AnnotatedLP.build(relBuilder.build(), TableType.STREAM, pk, unionTimestamp, select, inputs)
            .numRootPks(numRootPks).build());
  }

  @Override
  public RelNode visit(LogicalAggregate aggregate) {
    return setRelHolder(processAggregate(aggregate));
  }

  public AnnotatedLP processAggregate(LogicalAggregate aggregate) {
    //Need to inline TopN before we aggregate, but we postpone inlining now-filter in case we can push it through
    final AnnotatedLP input = getRelHolder(aggregate.getInput().accept(this))
        .inlineTopN(makeRelBuilder(), exec);
    errors.checkFatal(aggregate.groupSets.size() == 1, NOT_YET_IMPLEMENTED,
        "Do not yet support GROUPING SETS.");
    final List<Integer> groupByIdx = aggregate.getGroupSet().asList().stream()
        .map(idx -> input.select.map(idx))
        .collect(Collectors.toList());
    List<AggregateCall> aggregateCalls = mapAggregateCalls(aggregate, input);
    int targetLength = groupByIdx.size() + aggregateCalls.size();

    exec.requireAggregates(aggregateCalls);

    //Check if this a nested aggregation of a stream on root primary key during write stage
    if (input.type == TableType.STREAM && input.numRootPks.isPresent()
        && input.numRootPks.get() <= groupByIdx.size()
        && groupByIdx.subList(0, input.numRootPks.get())
        .equals(input.primaryKey.targetsAsList().subList(0, input.numRootPks.get()))) {
      return handleNestedAggregationInStream(input, groupByIdx, aggregateCalls);
    }

    //Check if this is a time-window aggregation (i.e. a roll-up)
    Pair<TimestampHolder.Derived.Candidate, Integer> timeKey;
    if (input.type == TableType.STREAM && input.getRelNode() instanceof LogicalProject
        && (timeKey = findTimestampInGroupBy(groupByIdx, input.timestamp, input.relNode))!=null) {
      return handleTimeWindowAggregation(input, groupByIdx, aggregateCalls, targetLength,
          timeKey.getLeft(), timeKey.getRight());
    }

    /* Check if any of the aggregate calls is a max(timestamp candidate) in which
       case that candidate should be the fixed timestamp and the result of the agg should be
       the resulting timestamp
     */
    Optional<TimestampAnalysis.MaxTimestamp> maxTimestamp = TimestampAnalysis.findMaxTimestamp(
        aggregateCalls, input.timestamp);

    //Check if we need to propagate timestamps
    if (input.type.isStream()) {
      return handleTimestampedAggregationInStream(aggregate, input, groupByIdx, aggregateCalls,
          maxTimestamp, targetLength);
    }

    return handleStandardAggregation(input, groupByIdx, aggregateCalls, maxTimestamp, targetLength);
  }



  private List<AggregateCall> mapAggregateCalls(LogicalAggregate aggregate, AnnotatedLP input) {
    List<AggregateCall> aggregateCalls = aggregate.getAggCallList().stream().map(agg -> {
      errors.checkFatal(agg.getCollation().getFieldCollations().isEmpty(), NOT_YET_IMPLEMENTED,
          "Ordering within aggregations is not yet supported: %s", agg);
      errors.checkFatal(agg.getCollation().getFieldCollations().isEmpty(), NOT_YET_IMPLEMENTED,
          "Filtering within aggregations is not yet supported: %s", agg);
      return agg.copy(
          agg.getArgList().stream().map(idx -> input.select.map(idx)).collect(Collectors.toList()));
    }).collect(Collectors.toList());
    return aggregateCalls;
  }

  private AnnotatedLP handleNestedAggregationInStream(AnnotatedLP input, List<Integer> groupByIdx,
      List<AggregateCall> aggregateCalls) {
    TimestampHolder.Derived.Candidate candidate = input.timestamp.getCandidates().stream()
        .filter(cand -> groupByIdx.contains(cand.getIndex())).findAny()
        .orElse(input.timestamp.getBestCandidate());

    RelBuilder relB = makeRelBuilder();
    relB.push(input.relNode);
    Pair<PkAndSelect, TimestampHolder.Derived> addedTimestamp =
        addTimestampAggregate(relB, groupByIdx, candidate, aggregateCalls);
    PkAndSelect pkSelect = addedTimestamp.getKey();
    TimestampHolder.Derived timestamp = addedTimestamp.getValue();

    TumbleAggregationHint.instantOf(candidate.getIndex()).addTo(relB);

    NowFilter nowFilter = input.nowFilter.remap(IndexMap.singleton(candidate.getIndex(),
        timestamp.getTimestampCandidate().getIndex()));

    return AnnotatedLP.build(relB.build(), TableType.STREAM, pkSelect.pk, timestamp, pkSelect.select,
                input)
            .numRootPks(Optional.of(pkSelect.pk.getSourceLength()))
            .nowFilter(nowFilter).build();
  }

  private Pair<TimestampHolder.Derived.Candidate, Integer> findTimestampInGroupBy(
      List<Integer> groupByIdx, TimestampHolder.Derived timestamp, RelNode input) {
    //Determine if one of the groupBy keys is a timestamp
    TimestampHolder.Derived.Candidate keyCandidate = null;
    int keyIdx = -1;
    for (int i = 0; i < groupByIdx.size(); i++) {
      int idx = groupByIdx.get(i);
      if (timestamp.isCandidate(idx)) {
        if (keyCandidate!=null) {
          errors.fatal(ErrorCode.NOT_YET_IMPLEMENTED, "Do not currently support grouping by "
              + "multiple timestamp columns: [%s] and [%s]",
              rexUtil.getFieldName(idx,input), rexUtil.getFieldName(keyCandidate.getIndex(),input));
        }
        keyCandidate = timestamp.getCandidateByIndex(idx);
        keyIdx = i;
        assert keyCandidate.getIndex() == idx;
      }
    }
    if (keyCandidate == null) return null;
    return Pair.of(keyCandidate, keyIdx);
  }

  private AnnotatedLP handleTimeWindowAggregation(AnnotatedLP input,
      List<Integer> groupByIdx, List<AggregateCall> aggregateCalls, int targetLength,
      TimestampHolder.Derived.Candidate keyCandidate, int keyIdx) {
    LogicalProject inputProject = (LogicalProject) input.getRelNode();
    RexNode timeAgg = inputProject.getProjects().get(keyCandidate.getIndex());
    TimeTumbleFunctionCall bucketFct = TimeTumbleFunctionCall.from(timeAgg,
        rexUtil.getBuilder()).orElseThrow(
        () -> errors.exception("Not a valid time aggregation function: %s", timeAgg)
    );

    //Fix timestamp (if not already fixed)
    TimestampHolder.Derived newTimestamp = keyCandidate.withIndex(keyIdx).fixAsTimestamp();
    //Now filters must be on the timestamp - this is an internal check
    Preconditions.checkArgument(input.nowFilter.isEmpty()
        || input.nowFilter.getTimestampIndex() == keyCandidate.getIndex());
    NowFilter nowFilter = input.nowFilter.remap(
        IndexMap.singleton(keyCandidate.getIndex(), keyIdx));

    RelBuilder relB = makeRelBuilder();
    relB.push(input.relNode);
    relB.aggregate(relB.groupKey(Ints.toArray(groupByIdx)), aggregateCalls);
    SqrlTimeTumbleFunction.Specification windowSpec = bucketFct.getSpecification();
    TumbleAggregationHint.functionOf(keyCandidate.getIndex(),
        bucketFct.getTimestampColumnIndex(),
        windowSpec.getWindowWidthMillis(), windowSpec.getWindowOffsetMillis()).addTo(relB);
    PkAndSelect pkSelect = aggregatePkAndSelect(groupByIdx, targetLength);

    /* TODO: this type of streaming aggregation requires a post-filter in the database (in physical model) to filter out "open" time buckets,
    i.e. time_bucket_col < time_bucket_function(now()) [if now() lands in a time bucket, that bucket is still open and shouldn't be shown]
      set to "SHOULD" once this is supported
     */

    return AnnotatedLP.build(relB.build(), TableType.STREAM, pkSelect.pk, newTimestamp,
                pkSelect.select, input)
            .numRootPks(Optional.of(pkSelect.pk.getSourceLength()))
            .nowFilter(nowFilter).build();
  }

  private AnnotatedLP handleTimestampedAggregationInStream(LogicalAggregate aggregate, AnnotatedLP input,
      List<Integer> groupByIdx, List<AggregateCall> aggregateCalls,
      Optional<TimestampAnalysis.MaxTimestamp> maxTimestamp, int targetLength) {

    //Fix best timestamp (if not already fixed)
    TimestampHolder.Derived inputTimestamp = input.timestamp;
    TimestampHolder.Derived.Candidate candidate = maxTimestamp.map(MaxTimestamp::getCandidate)
        .orElse(inputTimestamp.getBestCandidate());

    if (!input.nowFilter.isEmpty() && exec.supports(EngineCapability.STREAM_WINDOW_AGGREGATION)) {
      NowFilter nowFilter = input.nowFilter;
      //Determine timestamp, add to group-By and
      Preconditions.checkArgument(nowFilter.getTimestampIndex() == candidate.getIndex(),
          "Timestamp indexes don't match");
      errors.checkFatal(!groupByIdx.contains(candidate.getIndex()),
          "Cannot group on timestamp: %s", rexUtil.getFieldName(candidate.getIndex(), input.relNode));
      RelBuilder relB = makeRelBuilder();
      relB.push(input.relNode);
      Pair<PkAndSelect, TimestampHolder.Derived> addedTimestamp =
          addTimestampAggregate(relB, groupByIdx, candidate, aggregateCalls);
      PkAndSelect pkAndSelect = addedTimestamp.getKey();
      TimestampHolder.Derived timestamp = addedTimestamp.getValue();

      //Convert now-filter to sliding window and add as hint
      long intervalWidthMs = nowFilter.getPredicate().getIntervalLength();
      // TODO: extract slide-width from hint
      long slideWidthMs = intervalWidthMs / config.getSlideWindowPanes();
      errors.checkFatal(slideWidthMs > 0 && slideWidthMs < intervalWidthMs,
          "Invalid sliding window widths: %s - %s", intervalWidthMs, slideWidthMs);
      new SlidingAggregationHint(candidate.getIndex(), intervalWidthMs, slideWidthMs).addTo(relB);

      TopNConstraint dedup = TopNConstraint.makeDeduplication(pkAndSelect.pk.targetsAsList(),
          timestamp.getTimestampCandidate().getIndex());
      return AnnotatedLP.build(relB.build(), TableType.DEDUP_STREAM, pkAndSelect.pk,
              timestamp, pkAndSelect.select, input)
          .topN(dedup).build();
    } else {
      //Convert aggregation to window-based aggregation in a project so we can preserve timestamp followed by dedup
      AnnotatedLP nowInput = input.inlineNowFilter(makeRelBuilder(), exec);

      int selectLength = targetLength;
      //Adding timestamp column to output relation if not already present
      if (maxTimestamp.isEmpty()) targetLength += 1;

      RelNode inputRel = nowInput.relNode;
      RelBuilder relB = makeRelBuilder();
      relB.push(inputRel);

      RexInputRef timestampRef = RexInputRef.of(candidate.getIndex(), inputRel.getRowType());

      List<RexNode> partitionKeys = new ArrayList<>(groupByIdx.size());
      List<RexNode> projects = new ArrayList<>(targetLength);
      List<String> projectNames = new ArrayList<>(targetLength);
      //Add groupByKeys in a sorted order
      List<Integer> sortedGroupByKeys = groupByIdx.stream().sorted().collect(Collectors.toList());
      for (Integer keyIdx : sortedGroupByKeys) {
        RexInputRef ref = RexInputRef.of(keyIdx, inputRel.getRowType());
        projects.add(ref);
        projectNames.add(null);
        partitionKeys.add(ref);
      }
      RexFieldCollation orderBy = new RexFieldCollation(timestampRef, Set.of());

      //Add aggregate functions
      for (int i = 0; i < aggregateCalls.size(); i++) {
        AggregateCall call = aggregateCalls.get(i);
        RexNode agg = rexUtil.getBuilder().makeOver(call.getType(), call.getAggregation(),
            call.getArgList().stream()
                .map(idx -> RexInputRef.of(idx, inputRel.getRowType()))
                .collect(Collectors.toList()),
            partitionKeys,
            ImmutableList.of(orderBy),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            true, true, false, false, true
        );
        projects.add(agg);
        projectNames.add(aggregate.getNamedAggCalls().get(i).getValue());
      }

      TimestampHolder.Derived outputTimestamp;
      if (maxTimestamp.isPresent()) {
        outputTimestamp = candidate.withIndex(sortedGroupByKeys.size() + maxTimestamp.get().getAggCallIdx())
            .fixAsTimestamp();
      } else {
        //Add timestamp as last project
        outputTimestamp = candidate.withIndex(targetLength - 1)
            .fixAsTimestamp();
        projects.add(timestampRef);
        projectNames.add(null);
      }

      relB.project(projects, projectNames);
      PkAndSelect pkSelect = aggregatePkAndSelect(sortedGroupByKeys, sortedGroupByKeys, selectLength);
      TopNConstraint dedup = TopNConstraint.makeDeduplication(pkSelect.pk.targetsAsList(),
          outputTimestamp.getTimestampCandidate().getIndex());
      return AnnotatedLP.build(relB.build(), TableType.DEDUP_STREAM, pkSelect.pk,
          outputTimestamp, pkSelect.select, input).topN(dedup).build();
    }
  }

  private AnnotatedLP handleStandardAggregation(AnnotatedLP input,
      List<Integer> groupByIdx, List<AggregateCall> aggregateCalls,
      Optional<TimestampAnalysis.MaxTimestamp> maxTimestamp, int targetLength) {
    //Standard aggregation produces a state table
    input = input.inlineNowFilter(makeRelBuilder(), exec);
    RelBuilder relB = makeRelBuilder();
    relB.push(input.relNode);
    TimestampHolder.Derived resultTimestamp;
    int selectLength = targetLength;
    if (maxTimestamp.isPresent()) {
      TimestampAnalysis.MaxTimestamp mt = maxTimestamp.get();
      resultTimestamp = mt.getCandidate()
          .withIndex(groupByIdx.size() + mt.getAggCallIdx())
          .fixAsTimestamp();
    } else { //add max timestamp
      TimestampHolder.Derived.Candidate bestCand = input.timestamp.getBestCandidate();
      //New timestamp column is added at the end
      resultTimestamp = bestCand.withIndex(targetLength)
          .fixAsTimestamp();
      AggregateCall maxTimestampAgg = rexUtil.makeMaxAggCall(bestCand.getIndex(),
          ReservedName.SYSTEM_TIMESTAMP.getCanonical(), groupByIdx.size(), relB.peek());
      aggregateCalls.add(maxTimestampAgg);
      targetLength++;
    }

    relB.aggregate(relB.groupKey(Ints.toArray(groupByIdx)), aggregateCalls);
    PkAndSelect pkSelect = aggregatePkAndSelect(groupByIdx, selectLength);
    return AnnotatedLP.build(relB.build(), TableType.STATE, pkSelect.pk,
        resultTimestamp, pkSelect.select, input).build();
  }

  private Pair<PkAndSelect, TimestampHolder.Derived> addTimestampAggregate(
      RelBuilder relBuilder, List<Integer> groupByIdx, TimestampHolder.Derived.Candidate candidate,
      List<AggregateCall> aggregateCalls) {
    int targetLength = groupByIdx.size() + aggregateCalls.size();
    //if agg-calls return types are nullable because there are no group-by keys, we have to make then non-null
    if (groupByIdx.isEmpty()) {
      aggregateCalls = aggregateCalls.stream().map(agg -> CalciteUtil.makeNotNull(agg,
          relBuilder.getTypeFactory())).collect(Collectors.toList());
    }

    List<Integer> groupByIdxTimestamp = new ArrayList<>(groupByIdx);
    boolean addedTimestamp = !groupByIdxTimestamp.contains(candidate.getIndex());
    if (addedTimestamp) {
      groupByIdxTimestamp.add(candidate.getIndex());
      targetLength++;
    }
    Collections.sort(groupByIdxTimestamp);
    int newTimestampIdx = groupByIdxTimestamp.indexOf(candidate.getIndex());
    TimestampHolder.Derived timestamp = candidate.withIndex(newTimestampIdx).fixAsTimestamp();

    relBuilder.aggregate(relBuilder.groupKey(Ints.toArray(groupByIdxTimestamp)), aggregateCalls);
    //Restore original order of groupByIdx in primary key and select
    PkAndSelect pkAndSelect =
        aggregatePkAndSelect(groupByIdx, groupByIdxTimestamp, targetLength);
    return Pair.of(pkAndSelect, timestamp);
  }

  public PkAndSelect aggregatePkAndSelect(List<Integer> originalGroupByIdx,
      int targetLength) {
    return aggregatePkAndSelect(originalGroupByIdx,
        originalGroupByIdx.stream().sorted().collect(Collectors.toList()),
        targetLength);
  }

  /**
   * Produces the pk and select mappings by taking into consideration that the group-by indexes of
   * an aggregation are implicitly sorted because they get converted to a bitset in the RelBuilder.
   *
   * @param originalGroupByIdx The original list of selected group by indexes (may not be sorted)
   * @param finalGroupByIdx    The list of selected group by indexes to be used in the aggregate
   *                           (must be sorted)
   * @param targetLength       The number of columns of the aggregate operator
   * @return
   */
  public PkAndSelect aggregatePkAndSelect(List<Integer> originalGroupByIdx,
      List<Integer> finalGroupByIdx, int targetLength) {
    Preconditions.checkArgument(
        finalGroupByIdx.equals(finalGroupByIdx.stream().sorted().collect(Collectors.toList())),
        "Expected final groupByIdx to be sorted");
    ContinuousIndexMap.Builder pkBuilder = ContinuousIndexMap.builder(originalGroupByIdx.size());
    for (int idx : originalGroupByIdx) {
      int mappedToIdx = finalGroupByIdx.indexOf(idx);
      Preconditions.checkArgument(mappedToIdx >= 0, "Invalid groupByIdx [%s] to [%s]",
          originalGroupByIdx, finalGroupByIdx);
      pkBuilder.add(mappedToIdx);
    }
    ContinuousIndexMap pk = pkBuilder.build(targetLength);
    ContinuousIndexMap select = pk.append(ContinuousIndexMap.of(
        ContiguousSet.closedOpen(finalGroupByIdx.size(), targetLength)));
    return new PkAndSelect(pk, select);
  }

  @Value
  private static class PkAndSelect {

    ContinuousIndexMap pk;
    ContinuousIndexMap select;
  }


  @Override
  public RelNode visit(LogicalStream logicalStream) {
    AnnotatedLP input = getRelHolder(logicalStream.getInput().accept(this));
    return setRelHolder(makeStream(input, logicalStream.getStreamType()));
  }

  private AnnotatedLP makeStream(AnnotatedLP state, StreamType streamType) {
    errors.checkFatal(state.type.isState(),
        "Underlying table is already a stream");
    RelBuilder relBuilder = makeRelBuilder();
    state = state.dropSort().inlineNowFilter(relBuilder, exec).inlineTopN(relBuilder, exec);
    TimestampHolder.Derived.Candidate candidate = state.timestamp.getBestCandidate();
    TimestampHolder.Derived timestamp = state.timestamp.restrictTo(List.of(candidate.withIndex(1)));

    RelNode relNode = LogicalStream.create(state.relNode,streamType,
        new LogicalStreamMetaData(state.primaryKey.targetsAsArray(), state.select.targetsAsArray(), candidate.getIndex()));
    ContinuousIndexMap pk = ContinuousIndexMap.of(List.of(0));
    int numFields = relNode.getRowType().getFieldCount();
    ContinuousIndexMap select = ContinuousIndexMap.identity(numFields, numFields);
    exec.require(EngineCapability.TO_STREAM);
    return AnnotatedLP.build(relNode, TableType.STREAM, pk, timestamp,
        select, state).build();
  }

  @Override
  public RelNode visit(LogicalSort logicalSort) {
    errors.checkFatal(logicalSort.offset == null, NOT_YET_IMPLEMENTED,
        "OFFSET not yet supported");
    AnnotatedLP input = getRelHolder(logicalSort.getInput().accept(this));

    Optional<Integer> limit = getLimit(logicalSort.fetch);
    if (limit.isPresent()) {
      //Need to inline topN
      input = input.inlineTopN(makeRelBuilder(), exec);
    }

    //Map the collation fields
    RelCollation collation = logicalSort.getCollation();
    ContinuousIndexMap indexMap = input.select;
    RelCollation newCollation = indexMap.map(collation);

    AnnotatedLP result;
    if (limit.isPresent()) {
      result = input.copy()
          .topN(new TopNConstraint(List.of(), false, newCollation, limit,
              LPConverterUtil.getTimestampOrderIndex(newCollation, input.timestamp).isPresent())).build();
    } else {
      //We can just replace any old order that might be present
      result = input.copy().sort(new SortOrder(newCollation)).build();
    }
    return setRelHolder(result);
  }

  public Optional<Integer> getLimit(RexNode limit) {
    if (limit == null) {
      return Optional.empty();
    }
    Preconditions.checkArgument(limit instanceof RexLiteral);
    return Optional.of(((RexLiteral) limit).getValueAs(Integer.class));
  }

}
