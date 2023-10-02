/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.rules;

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
import com.datasqrl.plan.local.generate.AccessTableFunction;
import com.datasqrl.plan.local.generate.ComputeTableFunction;
import com.datasqrl.plan.local.generate.TableFunctionBase;
import com.datasqrl.plan.rel.LogicalStream;
import com.datasqrl.model.LogicalStreamMetaData;
import com.datasqrl.plan.rules.JoinAnalysis.Side;
import com.datasqrl.plan.rules.JoinAnalysis.Type;
import com.datasqrl.plan.rules.JoinTable.NormType;
import com.datasqrl.plan.rules.SQRLConverter.Config;
import com.datasqrl.plan.table.*;
import com.datasqrl.plan.table.VirtualRelationalTable.Child;
import com.datasqrl.plan.table.VirtualRelationalTable.Root;
import com.datasqrl.plan.util.SelectIndexMap;
import com.datasqrl.plan.util.SelectIndexMap.Builder;
import com.datasqrl.plan.util.PrimaryKeyMap;
import com.datasqrl.plan.util.TimeTumbleFunctionCall;
import com.datasqrl.plan.util.TimestampAnalysis;
import com.datasqrl.plan.util.TimestampAnalysis.MaxTimestamp;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.plan.util.IndexMap;
import com.datasqrl.util.SqrlRexUtil;
import com.datasqrl.plan.util.TimePredicate;
import com.datasqrl.plan.util.TimePredicateAnalyzer;
import com.datasqrl.util.SqrlRexUtil.JoinConditionDecomposition;
import com.datasqrl.util.SqrlRexUtil.JoinConditionDecomposition.EqualityCondition;
import com.google.common.base.Preconditions;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.calcite.rel.core.TableFunctionScan;
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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlKind;
import com.datasqrl.model.StreamType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.collections.ListUtils;
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

  @Override
  public RelNode visit(TableFunctionScan functionScan) {
    exec.require(EngineCapability.TABLE_FUNCTION_SCAN); //We only support table functions on the read side
    Optional<TableFunction> tableFunction = SqrlRexUtil.getCustomTableFunction(functionScan);
    errors.checkFatal(tableFunction.isPresent() && tableFunction.get() instanceof TableFunctionBase,
        "Invalid table function encountered in query: %s", functionScan);
    TableFunctionBase tblFct = (TableFunctionBase) tableFunction.get();
    config.getSourceTableConsumer().accept(tblFct);
    AnnotatedLP inputALP = tblFct.getAnalyzedLP().getConvertedRelnode();
    //TODO: We don't currently support nested types for table functions and hence don't have to do shredding
    Preconditions.checkArgument(!CalciteUtil.hasNesting(inputALP.getRelNode().getRowType()));

    final int numPrimaryKeys, numColumns, targetLength = inputALP.getRelNode().getRowType().getFieldCount();
    final TimestampHolder.Derived timestamp;
    if (tblFct instanceof AccessTableFunction) {
      AccessTableFunction accessFct = (AccessTableFunction) tblFct;
      VirtualRelationalTable vTable = (VirtualRelationalTable) accessFct.getTable().getVt();
      Preconditions.checkArgument(vTable.isRoot());
      numColumns = vTable.getNumColumns();
      numPrimaryKeys = vTable.getNumPrimaryKeys();
      timestamp = TimestampHolder.Base.ofDerived(inputALP.getTimestamp()).getDerived();
    } else {
      ComputeTableFunction computeFct = (ComputeTableFunction) tblFct;
      QueryRelationalTable queryTable = computeFct.getQueryTable();
      numPrimaryKeys = queryTable.getNumPrimaryKeys();
      numColumns = queryTable.getNumColumns();
      timestamp = queryTable.getTimestamp().getDerived();
    }

    return setRelHolder(new AnnotatedLP(functionScan, inputALP.getType(),
        PrimaryKeyMap.firstN(numPrimaryKeys),
        timestamp,
        SelectIndexMap.identity(numColumns, targetLength),
        (tblFct instanceof AccessTableFunction? inputALP.getJoinTables():Collections.EMPTY_LIST),
        Optional.of(inputALP.primaryKey.getLength()),
        NowFilter.EMPTY, TopNConstraint.EMPTY, //Can ignore those since they will be inlined
        inputALP.getSort(), List.of()));
  }

  private AnnotatedLP denormalizeTable(ScriptRelationalTable queryTable,
      VirtualRelationalTable vtable, int numColumns, Optional<Integer> numRootPks,
      boolean appendTimestamp) {
    //Shred the virtual table all the way to root:
    //First, we prepare all the data structures
    Builder indexMap = SelectIndexMap.builder(numColumns);
    PrimaryKeyMap.PrimaryKeyMapBuilder primaryKey = PrimaryKeyMap.builder();
    List<JoinTable> joinTables = new ArrayList<>();
    //Now, we shred
    RelNode relNode = shredTable(vtable, primaryKey, indexMap, joinTables, true).build();

    //Set timestamp
    TimestampHolder.Derived timestamp = queryTable.getTimestamp().getDerived();
    if (appendTimestamp) {
      indexMap.add(timestamp.getTimestampCandidate().getIndex());
    }

    PrimaryKeyMap pk = primaryKey.build();
    assert pk.getLength() == vtable.getNumPrimaryKeys();
    int targetLength = relNode.getRowType().getFieldCount();
    AnnotatedLP result = new AnnotatedLP(relNode, queryTable.getType(),
        pk,
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
        PrimaryKeyMap.firstN(vtable.getNumPrimaryKeys()),
        queryTable.getTimestamp().getDerived().remapIndexes(query2virtualTable),
        SelectIndexMap.identity(numColumns, targetLength),
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
        PrimaryKeyMap.firstN(vtable.getNumPrimaryKeys()),
        queryTable.getTimestamp().getDerived().remapIndexes(remapTimestamp),
        SelectIndexMap.identity(numColumns, targetLength),
         List.of(joinTable), numRootPks,
        NowFilter.EMPTY, TopNConstraint.EMPTY,
        SortOrder.EMPTY, List.of());
  }

  private RelBuilder shredTable(VirtualRelationalTable vtable,
                                PrimaryKeyMap.PrimaryKeyMapBuilder primaryKey,
                                SelectIndexMap.Builder select, List<JoinTable> joinTables,
                                boolean isLeaf) {
    Preconditions.checkArgument(joinTables.isEmpty());
    return shredTable(vtable, primaryKey, select, joinTables, null, JoinRelType.INNER, isLeaf);
  }

  private RelBuilder shredTable(VirtualRelationalTable vtable,
      PrimaryKeyMap.PrimaryKeyMapBuilder primaryKey,
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
                                PrimaryKeyMap.PrimaryKeyMapBuilder primaryKey,
                                SelectIndexMap.Builder select, List<JoinTable> joinTables,
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
      primaryKey.pkIndex(offset + i);
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

  private ShredTableResult shredChildTable(Child child, PrimaryKeyMap.PrimaryKeyMapBuilder primaryKey,
                                           SelectIndexMap.Builder select, List<JoinTable> joinTables,
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
      RexNode added = column.getExpression(path.mapLeafTable(), builder.peek().getRowType());
      rexUtil.appendColumn(builder, added, column.getNameId());
    }
  }

  public void constructLeafIndexMap(VirtualRelationalTable vtable,
                                    SelectIndexMap.Builder select, boolean isLeaf, Pair<JoinTable, RelBuilder> startingBase,
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

  public static SelectIndexMap constructIndexMap(RelDataType tableRowType) {
    int sourceLength = (int)tableRowType.getFieldList().stream().map(RelDataTypeField::getType)
            .filter(Predicate.not(CalciteUtil::isNestedTable)).count();
    SelectIndexMap.Builder selectBuilder = SelectIndexMap.builder(sourceLength);
    tableRowType.getFieldList().stream().filter(field -> !CalciteUtil.isNestedTable(field.getType()))
            .map(RelDataTypeField::getIndex).forEach(selectBuilder::add);
    return selectBuilder.build(tableRowType.getFieldCount());
  }

  private static final SqrlRexUtil.RexFinder FIND_NOW = SqrlRexUtil.findFunction(SqrlRexUtil::isNOW);

  @Override
  public RelNode visit(LogicalFilter logicalFilter) {
    AnnotatedLP input = getRelHolder(logicalFilter.getInput().accept(this));
    input = input.inlineTopN(makeRelBuilder(), exec); //Filtering doesn't preserve deduplication
    RexNode condition = logicalFilter.getCondition();
    condition = input.select.map(condition, input.relNode.getRowType());
    TimestampHolder.Derived timestamp = input.timestamp;
    NowFilter nowFilter = input.nowFilter;

    //Check if it has a now() predicate and pull out or throw an exception if malformed
    RelBuilder relBuilder = makeRelBuilder();
    relBuilder.push(input.relNode);
    List<TimePredicate> timeFunctions = new ArrayList<>();
    List<RexNode> conjunctions = rexUtil.getConjunctions(condition);

    //Identify any pk columns that are constrained by an equality constrained with a constant and remove from pk list
    Set<Integer> pksToRemove = new HashSet<>();
    for (RexNode node : conjunctions) {
      Optional<Integer> idxOpt = CalciteUtil.isEqualToConstant(node);
      idxOpt.filter(input.primaryKey::containsIndex).ifPresent(pksToRemove::add);
    }
    PrimaryKeyMap pk = input.primaryKey;
    if (!pksToRemove.isEmpty()) { //Remove them
      pk = PrimaryKeyMap.of(pk.asList().stream().filter(Predicate.not(pksToRemove::contains)).collect(
          Collectors.toList()));
    }

    if (FIND_NOW.foundIn(condition)) {
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
    return setRelHolder(input.copy().primaryKey(pk).relNode(relBuilder.build()).timestamp(timestamp)
        .nowFilter(nowFilter).build());
  }

  @Override
  public RelNode visit(LogicalProject logicalProject) {
    AnnotatedLP rawInput = getRelHolder(logicalProject.getInput().accept(this));

    SelectIndexMap trivialMap = getTrivialMapping(logicalProject, rawInput.select);
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
          limit = SqrlRexUtil.getLimit(nestedSort.fetch);
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

        PrimaryKeyMap pk = baseInput.primaryKey;
        SelectIndexMap select = trivialMap;
        TimestampHolder.Derived timestamp = baseInput.timestamp;
        boolean isDistinct = false;
        if (topNHint.getType() == TopNHint.Type.SELECT_DISTINCT) {
          List<Integer> distincts = SqrlRexUtil.combineIndexes(partition,
              trivialMap.targetsAsList());
          pk = PrimaryKeyMap.of(distincts);
          if (partition.isEmpty()) {
            //If there is no partition, we can ignore the sort order plus limit and turn this into a simple deduplication
            partition = pk.asList();
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

          pk = PrimaryKeyMap.of(partition);
          select = SelectIndexMap.identity(targetLength, targetLength); //Select everything
          //Extract timestamp from collation
          TimestampHolder.Derived.Candidate candidate = LPConverterUtil.getTimestampOrderIndex(
              collation, timestamp).get();
          collation = LPConverterUtil.getTimestampCollation(candidate);
          timestamp = candidate.fixAsTimestamp();
          limit = Optional.of(1); //distinct on has implicit limit of 1
        } else if (topNHint.getType() == TopNHint.Type.TOP_N) {
          //Prepend partition to primary key
          List<Integer> pkIdx = SqrlRexUtil.combineIndexes(partition, pk.asList());
          pk = PrimaryKeyMap.of(pkIdx);
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
      RexNode mapRex = input.select.map(exp.e, input.relNode.getRowType());
      updatedProjects.add(exp.i, mapRex);
      updatedNames.add(exp.i, logicalProject.getRowType().getFieldNames().get(exp.i));
      int originalIndex = -1;
      if (mapRex instanceof RexInputRef) { //Direct mapping
        originalIndex = (((RexInputRef) mapRex)).getIndex();
      } else { //Check for preserved timestamps and primary keys
        //1. Timestamps: Timestamps are preserved if they are mapped through timestamp-preserving functions
        Optional<TimestampHolder.Derived.Candidate> preservedCandidate = TimestampAnalysis.getPreservedTimestamp(
            mapRex, input.timestamp);
        //2. Primary Keys: Primary keys are preserved if they are mapped through coalesce with constant
        //   This would be done after an outer join to replace null values.
        Optional<Integer> coalescedWithConstant = CalciteUtil.isCoalescedWithConstant(mapRex);
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
        } else if (coalescedWithConstant.isPresent()) {
          originalIndex = coalescedWithConstant.get();
        }
      }
      if (originalIndex >= 0) {
        if (mappedProjects.putIfAbsent(originalIndex, exp.i) != null) {
          Optional<TimestampHolder.Derived.Candidate> originalCand = input.timestamp.getOptCandidateByIndex(
              originalIndex);
          originalCand.ifPresent(c -> timeCandidates.add(c.withIndex(exp.i)));
          //We are ignoring this mapping because the prior one takes precedence, let's see if we should warn the user
          if (input.primaryKey.containsIndex(originalIndex)) {
            //TODO: issue a warning to alert the user that this mapping is not considered part of primary key
            System.out.println("WARNING: mapping primary key multiple times");
          }
        }
      }
    }
    //Make sure we pull the primary keys and timestamp candidates through (i.e. append those to the projects
    //if not already present)
    PrimaryKeyMap.PrimaryKeyMapBuilder primaryKey = PrimaryKeyMap.builder();
    for (Integer pkIdx : input.primaryKey.asList()) {
      Integer target = mappedProjects.get(pkIdx);
      if (target == null) {
        //Need to add it
        target = updatedProjects.size();
        updatedProjects.add(target, RexInputRef.of(pkIdx, input.relNode.getRowType()));
        updatedNames.add(null);
        mappedProjects.put(pkIdx, target);
      }
      primaryKey.pkIndex(target);
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
    return setRelHolder(AnnotatedLP.build(newProject, input.type, primaryKey.build(),
            timestamp, SelectIndexMap.identity(logicalProject.getProjects().size(), fieldCount),
            input)
        .numRootPks(input.numRootPks).nowFilter(nowFilter).sort(sort).build());
  }

  private SelectIndexMap getTrivialMapping(LogicalProject project, SelectIndexMap baseMap) {
    SelectIndexMap.Builder b = SelectIndexMap.builder(project.getProjects().size());
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
//    JoinRelType joinType = logicalJoin.getJoinType();
    JoinAnalysis joinAnalysis = JoinAnalysis.of(logicalJoin.getJoinType());

    //We normalize the join by: 1) flipping right to left joins and 2) putting the stream on the left for stream-on-state joins
    if (joinAnalysis.isA(Side.RIGHT) ||
        (joinAnalysis.isA(Side.NONE) && leftInput.type.isState() && rightInput.type.isStream())) {
      joinAnalysis = joinAnalysis.flip();
      //Switch sides
      AnnotatedLP tmp = rightInput;
      rightInput = leftInput;
      leftInput = tmp;
    }
    assert joinAnalysis.isA(Side.LEFT) || joinAnalysis.isA(Side.NONE);

    final int leftSideMaxIdx = leftInput.getFieldLength();
    SelectIndexMap joinedIndexMap = leftInput.select.join(rightInput.select, leftSideMaxIdx, joinAnalysis.isFlipped());
    List<RelDataTypeField> inputFields = ListUtils.union(leftInput.relNode.getRowType().getFieldList(),
        rightInput.relNode.getRowType().getFieldList());
    RexNode condition = joinedIndexMap.map(logicalJoin.getCondition(), inputFields);
    exec.requireRex(condition);
    //TODO: pull now() conditions up as a nowFilter and move nested now filters through
    errors.checkFatal(!FIND_NOW.foundIn(condition),
        "now() is not allowed in join conditions");
    JoinConditionDecomposition eqDecomp = rexUtil.decomposeJoinCondition(
        condition, leftSideMaxIdx);

    //Identify if this is an identical self-join for a nested tree
    boolean hasTransformativePullups =
        !leftIn.topN.isEmpty() || !rightIn.topN.isEmpty();
    if ((joinAnalysis.canBe(Type.INNER) || joinAnalysis.canBe(Type.OUTER, Side.LEFT))
        && JoinTable.compatible(leftInput.joinTables, rightInput.joinTables)
        && !hasTransformativePullups
        && eqDecomp.getRemainingPredicates().isEmpty() && !eqDecomp.getTwoSidedEqualities().isEmpty()) {
      errors.checkFatal(JoinTable.getRoots(rightInput.joinTables).size() == 1,
          ErrorCode.NOT_YET_IMPLEMENTED,
          "Current assuming a single table on the right for self-join elimination.");
      errors.checkFatal(!joinAnalysis.isFlipped(),
          ErrorCode.NOT_YET_IMPLEMENTED,
          "RIGHT joins not yet supported between parent and child tables. Convert to LEFT join.");

      //Determine if we can map the tables from both branches of the join onto each-other
      Map<JoinTable, JoinTable> right2left;
      if (exec.supports(EngineCapability.DENORMALIZE)) {
        right2left = JoinTable.joinTreeMap(leftInput.joinTables, leftSideMaxIdx,
            rightInput.joinTables, eqDecomp.getTwoSidedEqualities());
      } else {
        right2left = JoinTable.joinListMap(leftInput.joinTables, leftSideMaxIdx,
            rightInput.joinTables, eqDecomp.getTwoSidedEqualities());
      }
      if (!right2left.isEmpty()) {
        JoinTable rightLeaf = Iterables.getOnlyElement(JoinTable.getLeafs(rightInput.joinTables));
        RelBuilder relBuilder = makeRelBuilder().push(leftInput.getRelNode());
        PrimaryKeyMap newPk = leftInput.primaryKey;
        List<JoinTable> joinTables = new ArrayList<>(leftInput.joinTables);
        joinAnalysis = joinAnalysis.makeGeneric();
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
          PrimaryKeyMap.PrimaryKeyMapBuilder addedPk = newPk.toBuilder();
          List<JoinTable> addedTables = new ArrayList<>();
          relBuilder = shredTable(rightLeaf.table, addedPk, addedTables,
              Pair.of(right2left.get(ancestor), relBuilder), joinAnalysis.export());
          newPk = addedPk.build();
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
          relBuilder.join(joinAnalysis.export(), condition);
          JoinTable rightTbl = rightLeaf.withOffset(leftSideMaxIdx);
          joinTables.add(rightTbl);
          right2left.put(rightLeaf,rightTbl);
          //Add any additional pks
          int rightNumPk = rightLeaf.getNumPkNormalized(), leftNumPk = leftTbl.getNumPkNormalized();
          if (rightNumPk > leftNumPk) {
            int deltaPk = rightNumPk - leftNumPk;
            PrimaryKeyMap.PrimaryKeyMapBuilder addedPk = newPk.toBuilder();
            for (int i = 0; i < deltaPk; i++) {
              addedPk.pkIndex(rightTbl.getGlobalIndex(leftNumPk + i));
            }
            newPk = addedPk.build();
          }
         }
        RelNode relNode = relBuilder.build();
        //Update indexMap based on the mapping of join tables
        final List<JoinTable> rightTables = rightInput.joinTables;
        SelectIndexMap remapedRight = rightInput.select.remap(
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


        SelectIndexMap indexMap = leftInput.select.append(remapedRight);
        return setRelHolder(AnnotatedLP.build(relNode, leftInput.type, newPk, leftInput.timestamp,
                indexMap, leftInput)
            .numRootPks(leftInput.numRootPks).nowFilter(resultNowFilter)
            .joinTables(joinTables).build());
      }
    }

    /*We are going to detect if all the pk columns on the left or right hand side of the join
      are covered by equality constraints since that determines the resulting pk and is used
      in temporal join detection */
    EnumMap<Side,Boolean> isPKConstrained = new EnumMap<>(Side.class);
    for (Side side : new Side[]{Side.LEFT, Side.RIGHT}) {
      AnnotatedLP constrainedInput;
      Function<EqualityCondition,Integer> getEqualityIdx;
      int idxOffset;
      if (side == Side.LEFT) {
        constrainedInput = leftInput;
        idxOffset = 0;
        getEqualityIdx = EqualityCondition::getLeftIndex;
      } else {
        assert side==Side.RIGHT;
        constrainedInput = rightInput;
        idxOffset = leftSideMaxIdx;
        getEqualityIdx = EqualityCondition::getRightIndex;
      }
      Set<Integer> pkIndexes = constrainedInput.primaryKey.asList().stream()
          .map(idx -> idx + idxOffset).collect(Collectors.toSet());
      Set<Integer> pkEqualities = eqDecomp.getEqualities().stream().map(getEqualityIdx)
          .collect(Collectors.toSet());
      isPKConstrained.put(side,pkEqualities.containsAll(pkIndexes));
    }



    //Detect temporal join
    if (joinAnalysis.canBe(Type.TEMPORAL)) {
      if (leftInput.type.isStream() && rightInput.type.isState()) {

        //Check for primary keys equalities on the state-side of the join
        if (isPKConstrained.get(Side.RIGHT) &&
            //eqDecomp.getRemainingPredicates().isEmpty() && eqDecomp.getEqualities().size() ==
            // rightInput.primaryKey.getSourceLength() && TODO: Do we need to ensure there are no other conditions?
            rightInput.nowFilter.isEmpty()) {
          RelBuilder relB = makeRelBuilder();
          relB.push(leftInput.relNode);
          if (rightInput.type!=TableType.DEDUP_STREAM && !exec.supports(EngineCapability.TEMPORAL_JOIN_ON_STATE)) {
            //Need to convert to stream and add dedup to make this work
            AnnotatedLP state2stream = makeStream(rightInput, StreamType.UPDATE);
            //stream has the same order of fields
//            TopNConstraint dedup = TopNConstraint.makeDeduplication(pkAndSelect.pk.targetsAsList(),
//                timestamp.getTimestampCandidate().getIndex());
            errors.checkFatal(false, ErrorCode.NOT_YET_IMPLEMENTED,
                "Currently temporal joins are limited to states that are deduplicated streams.");
          }

          relB.push(rightInput.relNode);
          Preconditions.checkArgument(rightInput.timestamp.hasFixedTimestamp());
          TimestampHolder.Derived joinTimestamp = leftInput.timestamp.getBestCandidate()
              .fixAsTimestamp();

          PrimaryKeyMap pk = leftInput.primaryKey.toBuilder().build();
          TemporalJoinHint hint = new TemporalJoinHint(
              joinTimestamp.getTimestampCandidate().getIndex(),
              rightInput.timestamp.getTimestampCandidate().getIndex(),
              rightInput.primaryKey.asArray());
          joinAnalysis = joinAnalysis.makeA(Type.TEMPORAL);
          relB.join(joinAnalysis.export(), condition);
          hint.addTo(relB);
          exec.require(EngineCapability.TEMPORAL_JOIN);
          return setRelHolder(AnnotatedLP.build(relB.build(), TableType.STREAM,
                  pk, joinTimestamp, joinedIndexMap,
                  List.of(leftInput, rightInput))
              .joinTables(leftInput.joinTables)
              .numRootPks(leftInput.numRootPks).nowFilter(leftInput.nowFilter).sort(leftInput.sort)
              .build());
        } else if (joinAnalysis.isA(Type.TEMPORAL)) {
          errors.fatal("Expected join condition to be equality condition on state's primary key.");
        }
      } else if (joinAnalysis.isA(Type.TEMPORAL)) {
        JoinAnalysis.Side side = joinAnalysis.getOriginalSide();
        errors.fatal("Expected %s side of the join to be stream and the other temporal state.",
                side==Side.NONE?"one":side.toString().toLowerCase());
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

    PrimaryKeyMap.PrimaryKeyMapBuilder concatPkBuilder;
    Set<Integer> joinedPKIdx = new HashSet<>();
    joinedPKIdx.addAll(leftInputF.primaryKey.asList());
    joinedPKIdx.addAll(rightInputF.primaryKey.remap(idx -> idx + leftSideMaxIdx).asList());
    for (EqualityCondition eq : eqDecomp.getEqualities()) {
      if (eq.isTwoSided()) {
        joinedPKIdx.remove(eq.getRightIndex());
      } else {
        joinedPKIdx.remove(eq.getOneSidedIndex());
      }

    }
    Side singletonSide = Side.NONE;
    for (Side side : new Side[]{Side.LEFT, Side.RIGHT}) {
      if (isPKConstrained.get(side)) singletonSide = side;
    }
    PrimaryKeyMap joinedPk = PrimaryKeyMap.of(joinedPKIdx.stream().sorted().collect(Collectors.toUnmodifiableList()));

    //combine sorts if present
    SortOrder joinedSort = leftInputF.sort.join(
        rightInputF.sort.remap(idx -> idx + leftSideMaxIdx));

    //Detect interval join
    if (leftInputF.type.isStream() && rightInputF.type.isStream()) {
      //Validate that the join condition includes time bounds on the timestamp columns of both sides of the join
      List<RexNode> conjunctions = new ArrayList<>(rexUtil.getConjunctions(condition));
      Predicate<Integer> isTimestampColumn = idx -> idx < leftSideMaxIdx
          ? leftInputF.timestamp.isCandidate(idx) :
          rightInputF.timestamp.isCandidate(idx - leftSideMaxIdx);
      //Convert time-based predicates to normalized form for analysis and get the ones that constrain both timestamps
      List<TimePredicate> timePredicates = conjunctions.stream().map(rex ->
              TimePredicateAnalyzer.INSTANCE.extractTimePredicate(rex, rexUtil.getBuilder(),
                  isTimestampColumn))
          .flatMap(Optional::stream).filter(tp -> !tp.hasTimestampFunction())
          //making sure predicate contains columns from both sides of the join
          .filter(tp -> (tp.getSmallerIndex() < leftSideMaxIdx) ^ (tp.getLargerIndex()
              < leftSideMaxIdx))
          .collect(Collectors.toList());
      /*
      Detect a special case where we are joining two child tables of the same root table (i.e. we
      have equality constraints on the root pk columns for both sides). In that case, we are guaranteed
      that the timestamps must be identical and we can add that condition to convert the join to
      an interval join.
       */
      Optional<Integer> numRootPks = Optional.empty();
      if (timePredicates.isEmpty() && leftInputF.numRootPks.flatMap(npk ->
          rightInputF.numRootPks.filter(npk2 -> npk2.equals(npk))).isPresent()) {
        //If both streams have same number of root primary keys, check if those are part of equality conditions
        List<EqualityCondition> rootPkPairs = new ArrayList<>();
        for (int i = 0; i < leftInputF.numRootPks.get(); i++) {
          rootPkPairs.add(new EqualityCondition(leftInputF.primaryKey.map(i),
              rightInputF.primaryKey.map(i) + leftSideMaxIdx));
        }
        if (eqDecomp.getEqualities().containsAll(rootPkPairs)) {
          //Change primary key to only include root pk once and add equality time condition because timestamps must be equal
          TimePredicate eqCondition = new TimePredicate(
              rightInputF.timestamp.getBestCandidate().getIndex() + leftSideMaxIdx,
              leftInputF.timestamp.getBestCandidate().getIndex(), SqlKind.EQUALS, 0);
          timePredicates.add(eqCondition);
          conjunctions.add(eqCondition.createRexNode(rexUtil.getBuilder(), idxResolver, false));

          numRootPks = leftInputF.numRootPks;
          //remove root pk columns from right side when combining primary keys
          concatPkBuilder = leftInputF.primaryKey.toBuilder();
          List<Integer> rightPks = rightInputF.primaryKey.asList();
          rightPks.subList(numRootPks.get(), rightPks.size()).stream()
              .map(idx -> idx + leftSideMaxIdx).forEach(concatPkBuilder::pkIndex);
          joinedPk = concatPkBuilder.build();

        }
      }
      /*
      If we have time predicates that constraint timestamps from both sides of the join this is an interval join.
      We lock in the timestamps and pick the timestamp that is the upper bound as the resulting timestamp.
       */
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
        /* Lock in timestamp candidates for both sides and propagate timestamp. For timestamp propagation,
           we pick the timestamp on the left or right side of the join if it is a LEFT or RIGHT join, respectively,
           or the timestamp that is the upper bound in the constraint.
           */
        for (int tidx : timestampIndexes) {
          TimestampHolder.Derived newTimestamp = apply2JoinSide(tidx, leftSideMaxIdx, leftInputF,
              rightInputF,
              (prel, idx) -> prel.timestamp.getCandidateByIndex(idx).withIndex(tidx)
                  .fixAsTimestamp());
          if (joinAnalysis.isA(Side.LEFT)) { //it can only be left or none because we flipped right joins
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
        joinAnalysis = joinAnalysis.makeA(Type.INTERVAL);
        relB.join(joinAnalysis.export(), condition); //Can treat as "standard" inner join since no modification is necessary in physical plan
        SqrlHintStrategyTable.INTERVAL_JOIN.addTo(relB);
        return setRelHolder(AnnotatedLP.build(relB.build(), TableType.STREAM,
            joinedPk, joinTimestamp, joinedIndexMap,
            List.of(leftInputF, rightInputF)).numRootPks(numRootPks).sort(joinedSort).build());
      } else if (joinAnalysis.isA(Type.INTERVAL)) {
        errors.fatal("Interval joins require time bounds on the timestamp columns in the join condition.");
      }
    } else if (joinAnalysis.isA(Type.INTERVAL)) {
      errors.fatal(
          "Interval joins are only supported between two streams.");
    }

    //We detected no special time-based joins, so make it a generic join
    joinAnalysis = joinAnalysis.makeGeneric();

    //Default inner join creates a state table
    relB.join(joinAnalysis.export(), condition);
    //Default joins without primary key constraints can be expensive, so we create a hint for the cost model
    new JoinCostHint(leftInputF.type, rightInputF.type, eqDecomp.getEqualities().size(), singletonSide).addTo(
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
        joinedPk, resultTimestamp, joinedIndexMap,
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
            makeRelBuilder(), null, exec, errors)) //The post-process makes sure the input relations are aligned (pk,selects,timestamps)
        .collect(Collectors.toList());
    Preconditions.checkArgument(inputs.size() > 0);

    Set<Boolean> isStreamTypes = inputs.stream().map(AnnotatedLP::getType).map(TableType::isStream)
        .collect(Collectors.toUnmodifiableSet());
    errors.checkFatal(isStreamTypes.size()==1, "The input tables for a union must all be streams or states, but cannot be a mixture of the two.");
    boolean isStream = Iterables.getOnlyElement(isStreamTypes);

    //In the following, we can assume that the input relations are aligned because we post-processed them
    RelBuilder relBuilder = makeRelBuilder();
    //Find the intersection of primary key indexes across all inputs
    Set<Integer> sharedPk = inputs.stream().map(AnnotatedLP::getPrimaryKey)
        .map(pk -> Set.copyOf(pk.asList()))
        .reduce((pk1, pk2) -> {
          Set<Integer> result = new HashSet<>(pk1);
          result.retainAll(pk2);
          return result;
        }).orElse(Collections.emptySet());

    SelectIndexMap select = inputs.get(0).select;
    Optional<Integer> numRootPks = inputs.get(0).numRootPks;
    int maxSelectIdx = Collections.max(select.targetsAsList()) + 1;
    List<Integer> selectIndexes = SqrlRexUtil.combineIndexes(sharedPk, select.targetsAsList());
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
      errors.checkFatal(select.equals(input.select),
          "Input streams select different columns");
      //Validate primary key and selects line up between the streams
      errors.checkFatal(selectIndexes.containsAll(input.primaryKey.asList()),
          "The tables in the union have different primary keys. UNION requires uniform primary keys.");
      timestampIndexes.retainAll(input.timestamp.getCandidateIndexes());
      numRootPks = numRootPks.flatMap(npk -> input.numRootPks.filter(npk2 -> npk.equals(npk2)));
    }

    TimestampHolder.Derived unionTimestamp = TimestampHolder.Derived.NONE;
    List<Integer> unionPk = new ArrayList<>();
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
      input.primaryKey.asList().stream().filter(idx -> !unionPk.contains(idx)).forEach(unionPk::add);

      relBuilder.push(input.relNode);
      CalciteUtil.addProjection(relBuilder, localSelectIndexes, localSelectNames);
      unionTimestamp = unionTimestamp.union(localTimestamp);
    }
    Preconditions.checkArgument(unionTimestamp.hasCandidates());
    relBuilder.union(true, inputs.size());
    PrimaryKeyMap newPk = PrimaryKeyMap.of(unionPk);
    if (!isStream) exec.require(EngineCapability.UNION_STATE);
    return setRelHolder(
        AnnotatedLP.build(relBuilder.build(), isStream?TableType.STREAM:TableType.STATE, newPk, unionTimestamp, select, inputs)
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
        .equals(input.primaryKey.asList().subList(0, input.numRootPks.get()))) {
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
            .numRootPks(Optional.of(pkSelect.pk.getLength()))
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
            .numRootPks(Optional.of(pkSelect.pk.getLength()))
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

      TopNConstraint dedup = TopNConstraint.makeDeduplication(pkAndSelect.pk.asList(),
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
      TopNConstraint dedup = TopNConstraint.makeDeduplication(pkSelect.pk.asList(),
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
    Preconditions.checkArgument(originalGroupByIdx.stream().map(finalGroupByIdx::indexOf).allMatch(idx ->idx >= 0),
        "Invalid groupByIdx [%s] to [%s]", originalGroupByIdx, finalGroupByIdx);
    PrimaryKeyMap pk = PrimaryKeyMap.of(originalGroupByIdx.stream().map(finalGroupByIdx::indexOf).collect(
            Collectors.toList()));
    assert pk.getLength() == originalGroupByIdx.size();

    ContiguousSet<Integer> additionalIdx = ContiguousSet.closedOpen(finalGroupByIdx.size(), targetLength);
    SelectIndexMap.Builder selectBuilder = SelectIndexMap.builder(pk.getLength() + additionalIdx.size());
    selectBuilder.addAll(pk.asList()).addAll(additionalIdx);
    return new PkAndSelect(pk, selectBuilder.build(targetLength));
  }

  @Value
  private static class PkAndSelect {

    PrimaryKeyMap pk;
    SelectIndexMap select;
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
        new LogicalStreamMetaData(state.primaryKey.asArray(), state.select.targetsAsArray(), candidate.getIndex()));
    PrimaryKeyMap pk = PrimaryKeyMap.firstN(1);
    int numFields = relNode.getRowType().getFieldCount();
    SelectIndexMap select = SelectIndexMap.identity(numFields, numFields);
    exec.require(EngineCapability.TO_STREAM);
    return AnnotatedLP.build(relNode, TableType.STREAM, pk, timestamp,
        select, state).build();
  }

  @Override
  public RelNode visit(LogicalSort logicalSort) {
    errors.checkFatal(logicalSort.offset == null, NOT_YET_IMPLEMENTED,
        "OFFSET not yet supported");
    AnnotatedLP input = getRelHolder(logicalSort.getInput().accept(this));

    Optional<Integer> limit = SqrlRexUtil.getLimit(logicalSort.fetch);
    if (limit.isPresent()) {
      //Need to inline topN
      input = input.inlineTopN(makeRelBuilder(), exec);
    }

    //Map the collation fields
    RelCollation collation = logicalSort.getCollation();
    SelectIndexMap indexMap = input.select;
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

}
