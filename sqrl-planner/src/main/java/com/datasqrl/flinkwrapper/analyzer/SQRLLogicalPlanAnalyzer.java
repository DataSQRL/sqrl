/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.flinkwrapper.analyzer;

import static com.datasqrl.error.ErrorCode.DISTINCT_ON_TIMESTAMP;
import static com.datasqrl.error.ErrorCode.NOT_YET_IMPLEMENTED;
import static com.datasqrl.error.ErrorCode.WRONG_INTERVAL_JOIN;
import static com.datasqrl.util.CalciteUtil.CAST_TRANSFORM;
import static com.datasqrl.util.CalciteUtil.COALESCE_TRANSFORM;

import com.datasqrl.calcite.SqrlRexUtil;
import com.datasqrl.calcite.SqrlRexUtil.JoinConditionDecomposition;
import com.datasqrl.calcite.SqrlRexUtil.JoinConditionDecomposition.EqualityCondition;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableType;
import com.datasqrl.plan.hints.JoinCostHint;
import com.datasqrl.plan.hints.JoinModifierHint;
import com.datasqrl.plan.hints.SlidingAggregationHint;
import com.datasqrl.plan.hints.SqrlHint;
import com.datasqrl.plan.hints.SqrlHintStrategyTable;
import com.datasqrl.plan.hints.TemporalJoinHint;
import com.datasqrl.plan.hints.TopNHint;
import com.datasqrl.plan.hints.TumbleAggregationHint;
import com.datasqrl.plan.local.generate.QueryTableFunction;
import com.datasqrl.plan.rules.AbstractSqrlRelShuttle;
import com.datasqrl.plan.rules.AnnotatedLP;
import com.datasqrl.plan.rules.ExecutionAnalysis;
import com.datasqrl.plan.rules.JoinAnalysis;
import com.datasqrl.plan.rules.JoinAnalysis.Side;
import com.datasqrl.plan.rules.JoinAnalysis.Type;
import com.datasqrl.plan.rules.LPConverterUtil;
import com.datasqrl.plan.rules.SqrlConverterConfig;
import com.datasqrl.plan.table.NowFilter;
import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.datasqrl.plan.table.PullupOperator;
import com.datasqrl.plan.table.QueryRelationalTable;
import com.datasqrl.plan.table.SortOrder;
import com.datasqrl.plan.table.Timestamps;
import com.datasqrl.plan.table.TopNConstraint;
import com.datasqrl.plan.util.IndexMap;
import com.datasqrl.plan.util.PrimaryKeyMap;
import com.datasqrl.plan.util.SelectIndexMap;
import com.datasqrl.plan.util.TimePredicate;
import com.datasqrl.plan.util.TimePredicateAnalyzer;
import com.datasqrl.plan.util.TimestampAnalysis;
import com.datasqrl.plan.util.TimestampAnalysis.MaxTimestamp;
import com.datasqrl.schema.NestedRelationship;
import com.datasqrl.util.CalciteUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.primitives.Ints;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.JoinModifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;

/**
 * The {@link SQRLLogicalPlanAnalyzer} rewrites the logical plan (i.e. {@link RelNode} produced by the transpiler.
 *
 * The logical plan rewriting serves a number of purposes:
 * <ul>
 *     <li>Convert convenience features introduced by the transpiler (e.g. {@code DISTINCT ON} statements or nested limits)
 *     to proper Relnodes that can be processed by engines. The transpiler adds hints to the relnodes that this class
 *     expands to proper relational algebra (e.g. convert {@code DISTINCT ON} to a partitioned window-over with filter on row_number).
 *     </li>
 *     <li>Keep track of primary key columns and timestamps which are needed to write row data to a database (i.e. to materialize
 *     data). This class attempts to infer the primary key and timestamp from the relational operator and expressions and
 *     pulls those columns through if the user does not explicitly select them.
 *     </li>
 *     <li>Keep track of the {@link TableType} of each Relnode so that we can infer optimizations and likely user intent
 *     for ambiguous relational operators. For example, a default join between a stream and a state table on the state table's primary
 *     key is rewritten as a temporal join since that is the most likely intent of the user. If the user does not want a
 *     temporal join, they can explicitly declare the join to be an inner join.
 *     The {@link TableType} is also used in cost modeling and picking the right connectors.
 *     In addition, we keep track of potential time tumbling windows in {@link AnnotatedLP#timestamp} to infer when a user
 *     intents to execute a time windowed operation (e.g. window aggregation, join, or TopN).
 *     The overall goal is to rewrite "normal" SQL that users are likely to write into optimized stream processing SQL
 *     that uses time windows where possible.
 *     </li>
 *     <li>Pull up relational operators that can be executed later in the pipeline/DAG without changing semantics.
 *     A key goal for DataSQRL is making it easier to materialize data in stream, but some operations like sorting or filtering
 *     on the current time are very expensive in the stream and cheap in the database (i.e. later in the pipeline/DAG).
 *     Hence, we try to identify and pull up such expensive operations to produce more optimal DAGs. See {@link PullupOperator}
 *     for the types of operators we pull up.
 *     For the same reason, we keep track of the {@link AnnotatedLP#streamRoot} when processing streams so we can determine
 *     when we are doing operations on a single stream record for nested data, so we can add a small time window for efficient
 *     on stream processing and avoid creating state.
 *     </li>
 * </ul>
 *
 *
 * We keep track of the metadata for a RelNode in the {@link AnnotatedLP} class. This class implements a visitor pattern
 * over RelNodes that creates the {@link AnnotatedLP} for each RelNode.
 * We try to process all RelNodes by computing the associated metadata and pullups in {@link AnnotatedLP}. If that is not
 * possible (e.g. certain set operations do not allow primary key inference) we treat a RelNode as a {@link TableType#RELATION}
 * and don't do any extra processing.
 *
 */
public class SQRLLogicalPlanAnalyzer extends AbstractSqrlRelShuttle<AnnotatedLP> {

  RelBuilder relBuilder;
  SqrlRexUtil rexUtil;
  SqrlConverterConfig config;
  ErrorCollector errors;

  ExecutionAnalysis exec;

  SQRLLogicalPlanAnalyzer(@NonNull RelBuilder relBuilder,  @NonNull SqrlConverterConfig config,
      @NonNull ErrorCollector errors) {
    this.relBuilder = relBuilder;
    this.rexUtil = new SqrlRexUtil(relBuilder.getTypeFactory());
    this.config = config;
    this.errors = errors;
    this.exec = config.getExecAnalysis();
  }

  private RelBuilder makeRelBuilder() {
    RelBuilder rel = relBuilder.transform(config -> config.withPruneInputOfAggregate(false));
    return rel;
  }

  @Override
  protected RelNode setRelHolder(AnnotatedLP relHolder) {
    TopNConstraint topN = relHolder.getTopN();
    if (topN.hasLimit(1) && !topN.isDescendingDeduplication()) { //Don't want to pull those up
      relHolder = relHolder.inlineTopN(relBuilder, exec);
    }
    RelNode result = super.setRelHolder(relHolder);
    //Some sanity checks
    Preconditions.checkArgument(!relHolder.type.hasTimestamp() || !relHolder.timestamp.is(
        Timestamps.Type.UNDEFINED),"Timestamp required");
    Preconditions.checkArgument(!relHolder.type.hasPrimaryKey() || !relHolder.primaryKey.isUndefined(),
        "Primary key required");
    errors.checkFatal(relHolder.type!=TableType.LOOKUP || result instanceof LogicalTableScan, "Lookup tables can only be used in temporal joins");
    return result;
  }

  private boolean isRelation(AnnotatedLP... inputs) {
    return Arrays.stream(inputs).anyMatch(input -> input.type==TableType.RELATION);
  }

  private RelNode processRelation(List<AnnotatedLP> inputs, RelNode node) {
    Preconditions.checkArgument(node.getInputs().size()==inputs.size());
    List<RelNode> newInputs = inputs.stream().map(alp -> alp.toRelation(makeRelBuilder(), exec)).map(AnnotatedLP::getRelNode)
            .collect(Collectors.toUnmodifiableList());

    RelNode newNode = node.copy(node.getTraitSet(), newInputs);
    int numColumns = newNode.getRowType().getFieldCount();
    return setRelHolder(AnnotatedLP.build(newNode, TableType.RELATION, PrimaryKeyMap.UNDEFINED, Timestamps.UNDEFINED,
        SelectIndexMap.identity(numColumns, numColumns), inputs).build());
  }

  @Override
  public RelNode visit(RelNode relNode) {
    if (relNode instanceof TableFunctionScan) {
      return visit((TableFunctionScan) relNode);
    }
    //TODO: add support for Snapshot
    //By default, we process a relnode as a RELATION type
    List<AnnotatedLP> inputs = relNode.getInputs().stream()
            .map(input -> getRelHolder(input.accept(this))).collect(Collectors.toUnmodifiableList());
    return processRelation(inputs,relNode);
  }

  @Override
  public RelNode visit(TableScan tableScan) {
    return setRelHolder(processTableScan(tableScan));
  }

  public AnnotatedLP processTableScan(TableScan tableScan) {
    //The base scan tables for all SQRL queries are PhysicalRelationalTable
    PhysicalRelationalTable table = tableScan.getTable().unwrap(PhysicalRelationalTable.class);
    Preconditions.checkArgument(table != null);
    return createAnnotatedRootTable(tableScan, table);
  }

  private AnnotatedLP createAnnotatedRootTable(RelNode relNode, PhysicalRelationalTable table) {
    config.getSourceTableConsumer().accept(table);
    table.lock();
    int numColumns = table.getNumColumns();
    PullupOperator.Container pullups = table.getPullups();

    TopNConstraint topN = pullups.getTopN();
    if (exec.isMaterialize(table) && topN.isDescendingDeduplication()) {
      // We can drop topN since that gets enforced by writing to DB with primary key
      topN = TopNConstraint.EMPTY;
    }
    Optional<PhysicalRelationalTable> rootTable = table.getStreamRoot();
    if (CalciteUtil.hasNestedTable(table.getRowType())) {
      exec.requireFeature(EngineFeature.DENORMALIZE);
    };
    return new AnnotatedLP(relNode, table.getType(),
        table.getPrimaryKey().toKeyMap(),
        table.getTimestamp(),
        SelectIndexMap.identity(numColumns, numColumns),
        rootTable,
        pullups.getNowFilter(), topN,
        pullups.getSort(), List.of());
  }

  @Override
  public RelNode visit(TableFunctionScan functionScan) {
    Optional<TableFunction> tableFunctionOpt = SqrlRexUtil.getCustomTableFunction(functionScan);
    errors.checkFatal(tableFunctionOpt.isPresent(), "Invalid table function encountered in query: %s", functionScan);
    TableFunction tableFunction = tableFunctionOpt.get();
    if (tableFunction instanceof NestedRelationship) {
      NestedRelationship nestedRel = (NestedRelationship)tableFunction;
      RexCall call = (RexCall) functionScan.getCall();
      Preconditions.checkArgument(call.getOperands().size()==1);
      int numColumns = nestedRel.getRowType().getFieldCount();
      return setRelHolder(new AnnotatedLP(functionScan, TableType.STATIC,
          PrimaryKeyMap.of(nestedRel.getLocalPKs()),
          Timestamps.UNDEFINED,
          SelectIndexMap.identity(numColumns, numColumns),
          Optional.empty(),
          NowFilter.EMPTY, TopNConstraint.EMPTY,
          SortOrder.EMPTY, List.of()));
    } else if (tableFunction instanceof QueryTableFunction) {
      exec.requireFeature(EngineFeature.TABLE_FUNCTION_SCAN); //We only support table functions on the read side
      QueryTableFunction tblFct = (QueryTableFunction) tableFunction;
      QueryRelationalTable queryTable = tblFct.getQueryTable();
      return setRelHolder(createAnnotatedRootTable(functionScan, queryTable));
    } else {
      //Generic table function call
      RelDataType rowType = functionScan.getRowType();
      return setRelHolder(new AnnotatedLP(functionScan, TableType.STATIC,
          determinePK(rowType),
          Timestamps.UNDEFINED,
          SelectIndexMap.identity(rowType.getFieldCount(), rowType.getFieldCount()),
          Optional.empty(),
          NowFilter.EMPTY, TopNConstraint.EMPTY,
          SortOrder.EMPTY, List.of()));
    }
  }

  @Override
  public RelNode visit(LogicalValues logicalValues) {
    PrimaryKeyMap pk = determinePK(logicalValues);
    int numCols = logicalValues.getRowType().getFieldCount();
    return setRelHolder(AnnotatedLP.build(logicalValues,TableType.STATIC, pk,
            Timestamps.UNDEFINED,SelectIndexMap.identity(numCols, numCols), List.of()).build());
  }

  private PrimaryKeyMap determinePK(LogicalValues logicalValues) {
    ImmutableList<ImmutableList<RexLiteral>> tuples = logicalValues.getTuples();
    if (tuples.size()<=1) return PrimaryKeyMap.none();
    return determinePK(logicalValues.getRowType());
  }

  /**
   * Determines the primary key of a table by rowtype using the simple heuristic of selecting
   * the first non-null scalar column.
   *
   * @param rowType
   * @return
   */
  private PrimaryKeyMap determinePK(RelDataType rowType) {
    List<RelDataTypeField> fields = rowType.getFieldList();
    for (int i = 0; i < fields.size(); i++) {
      RelDataType type = fields.get(i).getType();
      if (!CalciteUtil.isPotentialPrimaryKeyType(type)) continue;
      return PrimaryKeyMap.of(List.of(i));
    }
    throw errors.exception("Could not identify a primary key column for row type: %s", rowType);

  }

  private static final SqrlRexUtil.RexFinder FIND_NOW = SqrlRexUtil.findFunction(SqrlRexUtil::isNOW);

  @Override
  public RelNode visit(LogicalFilter logicalFilter) {
    AnnotatedLP input = getRelHolder(logicalFilter.getInput().accept(this));
    if (isRelation(input)) return processRelation(List.of(input),logicalFilter);

    input = input.inlineTopN(makeRelBuilder(), exec); //Filtering doesn't preserve deduplication
    RexNode condition = logicalFilter.getCondition();
    condition = input.select.map(condition, input.relNode.getRowType());
    Timestamps timestamp = input.timestamp;
    NowFilter nowFilter = input.nowFilter;

    //Check if it has a now() predicate and pull out or throw an exception if malformed
    RelBuilder relBuilder = makeRelBuilder();
    relBuilder.push(input.relNode);
    List<TimePredicate> timeFunctions = new ArrayList<>();
    List<RexNode> conjunctions = rexUtil.getConjunctions(condition);


    //Identify any columns that are constrained to a constant value and a) remove as pk if they are or b) update pk if filter is on row_number
    Set<PrimaryKeyMap.ColumnSet> pksToRemove = new HashSet<>();
    List<Integer> newPk = null;
    for (RexNode node : conjunctions) {
      Optional<Integer> idxOpt = CalciteUtil.isEqualToConstant(node);
      //It's a constrained primary key, remove it from the list
      idxOpt.flatMap(input.primaryKey::getForIndex).ifPresent(pksToRemove::add);
      //Check if this is the row_number of a partitioned window-over
      if (idxOpt.isPresent() && input.relNode instanceof LogicalProject) {
        RexNode column = ((LogicalProject) input.relNode).getProjects().get(idxOpt.orElseThrow());
        if (column instanceof RexOver && column.isA(SqlKind.ROW_NUMBER)) {
          newPk = ((RexOver)column).getWindow().partitionKeys.stream().map(n ->
                  CalciteUtil.getInputRefThroughTransform(n, List.of(CAST_TRANSFORM, COALESCE_TRANSFORM)))
              .map(opt -> opt.orElse(-1)).collect(Collectors.toUnmodifiableList());
          if (newPk.stream().anyMatch(idx -> idx<0)) {
            newPk = null; // Not all partition RexNodes are input refs
          }
        }
      }

    }
    PrimaryKeyMap pk = input.primaryKey;
    TableType type = input.getType();
    if (newPk != null) {
      pk = PrimaryKeyMap.of(newPk);
      if (type==TableType.STREAM) { //Update type
        type = TableType.VERSIONED_STATE;
      }
    } else if (!pksToRemove.isEmpty()) { //Remove them
      pk = new PrimaryKeyMap(pk.asList().stream().filter(Predicate.not(pksToRemove::contains)).collect(
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
      timestamp = Timestamps.ofFixed(timestampIdx);
      //Add as static time filter (to push down to source)
      NowFilter localNowFilter = combinedFilter.get();
      //TODO: add back in, push down to push into source, then remove
      //localNowFilter.addFilterTo(relBuilder,true);
    } else {
      conjunctions = List.of(condition);
    }
    relBuilder.filter(conjunctions);
    exec.requireRex(conjunctions);
    return setRelHolder(input.copy().primaryKey(pk).type(type).relNode(relBuilder.build()).timestamp(timestamp)
        .nowFilter(nowFilter).build());
  }

  @Override
  public RelNode visit(LogicalProject logicalProject) {
    //Check if this is a topN constraint which requires special processing unique to SQRL
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

      Pair<SelectIndexMap, Boolean> trivialMapResult = getTrivialMapping(logicalProject, baseInput.select);
      Preconditions.checkArgument(trivialMapResult!=null && !trivialMapResult.getRight(), "Select must be trivial and not rename");

      PrimaryKeyMap pk = baseInput.primaryKey;
      SelectIndexMap select = trivialMapResult.getKey();
      Timestamps timestamp = baseInput.timestamp;
      RelBuilder relB = makeRelBuilder();
      relB.push(baseInput.relNode);
      boolean isSelectDistinct = false;
      if (topNHint.getType() == TopNHint.Type.SELECT_DISTINCT) {
        List<Integer> distincts = SqrlRexUtil.combineIndexes(partition,
            select.targetsAsList());
        pk = PrimaryKeyMap.of(distincts);
        if (partition.isEmpty()) {
          //If there is no partition, we can ignore the sort order plus limit and turn this into a simple deduplication
          partition = distincts;
          if (timestamp.hasCandidates()) {
            timestamp = Timestamps.ofFixed(timestamp.getBestCandidate(relB));
            collation = LPConverterUtil.getTimestampCollation(timestamp.getOnlyCandidate());
          } else {
            collation = RelCollations.of(distincts.get(0));
          }
          limit = Optional.of(1);
        } else {
          isSelectDistinct = true;
        }
      } else if (topNHint.getType() == TopNHint.Type.DISTINCT_ON) {
        Preconditions.checkArgument(!partition.isEmpty());
        Preconditions.checkArgument(!collation.getFieldCollations().isEmpty());
        Optional<Integer> timestampOrder = LPConverterUtil.getTimestampOrderIndex(collation, timestamp);
        if (config.isFilterDistinctOrder()) {
          if (timestampOrder.isPresent()) {
            errors.notice("No filtering necessary since timestamp order is used.");
          } else {
            //Check conditions for filtered distinct
            errors.checkFatal(baseInput.type.isStream(),"Filtered distinct only supported on streams");
            errors.checkFatal(collation.getFieldCollations().size()==1, "Filtered distinct requires a single order column");
            RelFieldCollation fieldCol = collation.getFieldCollations().get(0);
            errors.checkFatal(fieldCol.direction.isDescending(), "Filtered distinct requires descending order");
            errors.checkFatal(timestamp.size()==1, "Filtered distinct requires a single timestamp column, but found: %", timestamp); //TODO: replace with fieldname & index

            CalciteUtil.addFilteredDeduplication(relB, timestamp.getOnlyCandidate(), partition, fieldCol.getFieldIndex());
          }
        }
        if (baseInput.type.isStream()) {
          if (timestampOrder.isEmpty()) {
            errors.notice(DISTINCT_ON_TIMESTAMP,
                "DISTINCT ON statements is not ordered by timestamp");
          } else {
            timestamp = Timestamps.ofFixed(timestampOrder.get());
          }
        }
        pk = PrimaryKeyMap.of(partition);
        select = SelectIndexMap.identity(targetLength, targetLength); //Select everything
        limit = Optional.of(1); //distinct on has implicit limit of 1
      } else if (topNHint.getType() == TopNHint.Type.TOP_N) {
        //Prepend partition to primary key
        PrimaryKeyMap.Builder pkBuilder = PrimaryKeyMap.build();
        partition.forEach(pkBuilder::add);
        pkBuilder.addAllNotOverlapping(pk.asList());
        pk = pkBuilder.build();
      }

      TopNConstraint topN = new TopNConstraint(partition, isSelectDistinct, collation, limit,
          LPConverterUtil.getTimestampOrderIndex(collation, timestamp).isPresent());
      TableType resultType = TableType.STATE;
      if (baseInput.type.isStream() && topN.isDeduplication()) resultType = TableType.VERSIONED_STATE;
      return setRelHolder(baseInput.copy().relNode(relB.build()).type(resultType)
          .primaryKey(pk).select(select).timestamp(timestamp)
          .topN(topN).sort(SortOrder.EMPTY).build());
    }
    AnnotatedLP rawInput = getRelHolder(logicalProject.getInput().accept(this));
    if (isRelation(rawInput)) return processRelation(List.of(rawInput),logicalProject);
    Pair<SelectIndexMap, Boolean> trivialMapResult = getTrivialMapping(logicalProject, rawInput.select);
    if (trivialMapResult!=null && !trivialMapResult.getRight()) {
      //Inline trivial selects but only if it doesn't rename, so we don't lose names
      return setRelHolder(rawInput.copy().select(trivialMapResult.getKey()).build());
    }
    //Inline topN if it is not a descending deduplication which we want to pull through
    AnnotatedLP input = rawInput;
    if (!rawInput.getTopN().isDescendingDeduplication()) {
      input = rawInput.inlineTopN(makeRelBuilder(), exec);
    }
    //Update index mappings
    List<RexNode> updatedProjects = new ArrayList<>();
    List<String> updatedNames = new ArrayList<>();
    //We only keep track of the first mapped project and consider it to be the "preserving one" for primary keys and timestamps
    LinkedHashMultimap<Integer, Integer> mappedProjects = LinkedHashMultimap.create();
    RelDataType rowType = input.relNode.getRowType();
    BiFunction<Integer, RelDataType, Integer> preserveIndex = (originalIndex, inputType) -> {
      Set<Integer> mapsTo = mappedProjects.get(originalIndex);
      if (mapsTo.isEmpty()) {
        int newIndex = updatedProjects.size();
        updatedProjects.add(newIndex, RexInputRef.of(originalIndex, inputType));
        updatedNames.add(null);
        mappedProjects.put(originalIndex, newIndex);
        return newIndex;
      } else {
        return mapsTo.stream().findFirst().get();
      }
    };
    Set<Timestamps.TimeWindow> timeWindows = new HashSet<>();
    NowFilter nowFilter = NowFilter.EMPTY;
    Timestamps.TimestampsBuilder timestampBuilder = Timestamps.build(input.timestamp.getType());
    for (Ord<RexNode> exp : Ord.<RexNode>zip(logicalProject.getProjects())) {
      RexNode mapRex = input.select.map(exp.e, input.relNode.getRowType());
      updatedProjects.add(exp.i, mapRex);
      updatedNames.add(exp.i, logicalProject.getRowType().getFieldNames().get(exp.i));
      Optional<Integer> inputRef = CalciteUtil.getNonAlteredInputRef(mapRex);
      Optional<Timestamps.TimeWindow> timeWindow;
      if (inputRef.isPresent()) { //Direct mapping
        int originalIndex = inputRef.get();
        if (input.timestamp.isCandidate(originalIndex)) timestampBuilder.candidate(exp.i);
        mappedProjects.put(originalIndex, exp.i);
      } else if (TimestampAnalysis.computesTimestamp(mapRex, input.timestamp)) {
        //Check for preserved timestamps through certain function calls
        timestampBuilder.candidate(exp.i);
      } else if ((timeWindow = TimestampAnalysis.extractTumbleWindow(exp.i, mapRex, rexUtil.getBuilder(), input.timestamp)).isPresent()) {
        timeWindows.add(timeWindow.get());
      }
    }
    //Make sure we pull the primary keys through (i.e. append those to the projects if not already present)
    //and keep track of primary keys that are mapped multiple times
    PrimaryKeyMap.Builder primaryKey = PrimaryKeyMap.build();
    for (PrimaryKeyMap.ColumnSet colSet : input.primaryKey.asList()) {
      Set<Integer> mappedTo = colSet.getIndexes().stream().flatMap(idx -> mappedProjects.get(idx).stream()).collect(Collectors.toUnmodifiableSet());
      if (mappedTo.isEmpty()) {
        primaryKey.add(preserveIndex.apply(colSet.pickBest(input.relNode.getRowType()),rowType));
      } else {
        primaryKey.add(mappedTo);
      }
    }

    RelBuilder relB = makeRelBuilder();
    relB.push(input.relNode);

    //Update now-filter because timestamp are updated
    if (!input.nowFilter.isEmpty()) {
      //Preserve now-filter
      int oldTimestampIdx = input.nowFilter.getTimestampIndex();
      int newTimestampIdx = preserveIndex.apply(oldTimestampIdx,rowType);
      timestampBuilder.candidate(newTimestampIdx);
      nowFilter = input.nowFilter.remap(IndexMap.singleton(oldTimestampIdx, newTimestampIdx));
    }
    //NowFilter must have been preserved
    assert !nowFilter.isEmpty() || input.nowFilter.isEmpty();

    //Preserve sorts
    List<RelFieldCollation> collations = input.sort.getCollation().getFieldCollations().stream()
            .map(collation -> collation.withFieldIndex(preserveIndex.apply(collation.getFieldIndex(),rowType)))
            .collect(Collectors.toUnmodifiableList());
    SortOrder sort = new SortOrder(RelCollations.of(collations));


    //Add windows and make sure we map timestamps through
    for (Timestamps.TimeWindow window : timeWindows) {
      int mapsTo = preserveIndex.apply(window.getTimestampIndex(),rowType);
      timestampBuilder.candidate(mapsTo);
      timestampBuilder.window(window.withTimestampIndex(mapsTo));
    }
    Timestamps timestamp = timestampBuilder.build();
    //Make sure we are pulling at least one viable timestamp candidates through
    if (input.timestamp.hasCandidates() && !timestamp.hasCandidates()) {
      int oldTimestampIdx = input.timestamp.getBestCandidate(relB);
      int newTimestampIdx = preserveIndex.apply(oldTimestampIdx, relB.peek().getRowType());
      timestampBuilder.candidate(newTimestampIdx);
      timestamp = timestampBuilder.build();
    }

    //Build new project
    relB.project(updatedProjects, updatedNames, true);
    RelNode newProject = relB.build();
    int fieldCount = updatedProjects.size();
    exec.requireRex(updatedProjects);
    TopNConstraint topN = TopNConstraint.EMPTY; //Was inline above unless is deduplication
    if (rawInput.getTopN().isDescendingDeduplication()) {
      topN = rawInput.getTopN().remap(idx ->
          mappedProjects.get(idx).stream().mapToInt(Integer::valueOf).min().orElse(-1));
    }
    return setRelHolder(AnnotatedLP.build(newProject, input.type, primaryKey.build(),
            timestamp, SelectIndexMap.identity(logicalProject.getProjects().size(), fieldCount),
            input)
        .streamRoot(input.streamRoot).topN(topN).nowFilter(nowFilter).sort(sort).build());
  }

  private Pair<SelectIndexMap,Boolean> getTrivialMapping(LogicalProject project, SelectIndexMap baseMap) {
    SelectIndexMap.Builder b = SelectIndexMap.builder(project.getProjects().size());
    List<String> names = project.getRowType().getFieldNames();
    List<String> inputNames = project.getInput().getRowType().getFieldNames();
    boolean isRenamed = false;
    for (Ord<RexNode> exp : Ord.<RexNode>zip(project.getProjects())) {
      RexNode rex = exp.e;
      if (!(rex instanceof RexInputRef)) {
        return null;
      }
      int index = (((RexInputRef) rex)).getIndex();
      String name = names.get(exp.i);
      if (!name.equals(inputNames.get(index)) && !name.startsWith(ReservedName.SYSTEM_PRIMARY_KEY.getDisplay())) {
        isRenamed = true;
      }
      b.add(baseMap.map(index));
    }
    return Pair.of(b.build(),isRenamed);
  }

  @Override
  public RelNode visit(LogicalJoin logicalJoin) {
    AnnotatedLP leftIn = getRelHolder(logicalJoin.getLeft().accept(this));
    AnnotatedLP rightIn = getRelHolder(logicalJoin.getRight().accept(this));
    if (isRelation(leftIn, rightIn)) return processRelation(List.of(leftIn, rightIn), logicalJoin);

    AnnotatedLP leftInput = leftIn.inlineTopN(makeRelBuilder(), exec);
    AnnotatedLP rightInput = rightIn.inlineTopN(makeRelBuilder(), exec);
//    JoinRelType joinType = logicalJoin.getJoinType();
    JoinAnalysis joinAnalysis = JoinAnalysis.of(logicalJoin.getJoinType(),
        getJoinModifier(logicalJoin.getHints()));

    //We normalize the join by: 1) flipping right to left joins and 2) putting the stream on the left for stream-on-x joins
    if (joinAnalysis.isA(Side.RIGHT) ||
        (joinAnalysis.isA(Side.NONE) && !leftInput.type.isStream() && rightInput.type.isStream())) {
      joinAnalysis = joinAnalysis.flip();
      //Switch sides
      AnnotatedLP tmp = rightInput;
      rightInput = leftInput;
      leftInput = tmp;
    }
    assert joinAnalysis.isA(Side.LEFT) || joinAnalysis.isA(Side.NONE);

    final int leftSideMaxIdx = leftInput.getFieldLength();
    final IndexMap remapRightSide = idx -> idx + leftSideMaxIdx;
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

    /*We are going to detect if all the pk columns on the left or right hand side of the join
      are covered by equality constraints since that determines the resulting pk and is used
      in temporal join detection */
    EnumMap<Side,Boolean> isPKConstrained = new EnumMap<>(Side.class);
    for (Side side : new Side[]{Side.LEFT, Side.RIGHT}) {
      AnnotatedLP constrainedInput;
      Function<EqualityCondition,Integer> getEqualityIdx;
      IndexMap remapPk;
      if (side == Side.LEFT) {
        constrainedInput = leftInput;
        remapPk = IndexMap.IDENTITY;
        getEqualityIdx = EqualityCondition::getLeftIndex;
      } else {
        assert side==Side.RIGHT;
        constrainedInput = rightInput;
        remapPk = remapRightSide;
        getEqualityIdx = EqualityCondition::getRightIndex;
      }
      Set<Integer> pkEqualities = eqDecomp.getEqualities().stream().map(getEqualityIdx)
          .collect(Collectors.toSet());
      boolean allCovered = constrainedInput.primaryKey.asList().stream()
              .map(col -> col.remap(remapPk)).allMatch(col -> col.containsAny(pkEqualities));
      isPKConstrained.put(side,allCovered);
    }



    //Detect temporal join
    if (joinAnalysis.canBe(Type.TEMPORAL)) {
      if (leftInput.type.isStream() && (rightInput.type==TableType.VERSIONED_STATE || rightInput.type==TableType.LOOKUP)) {
        //Check for primary keys equalities on the state-side of the join
        if (isPKConstrained.get(Side.RIGHT) && rightInput.nowFilter.isEmpty()) {
          RelBuilder relB = makeRelBuilder();
          relB.push(leftInput.relNode);

          Timestamps joinTimestamp = leftInput.timestamp.asBest(relB);
          relB.push(rightInput.relNode);

          PrimaryKeyMap pk = leftInput.primaryKey.toBuilder().build();
          TemporalJoinHint hint = new TemporalJoinHint(
              joinTimestamp.getOnlyCandidate());
          joinAnalysis = joinAnalysis.makeA(Type.TEMPORAL);
          relB.join(joinAnalysis.export(), condition);
          hint.addTo(relB);
          exec.requireFeature(EngineFeature.TEMPORAL_JOIN);
          return setRelHolder(AnnotatedLP.build(relB.build(), TableType.STREAM,
                  pk, joinTimestamp, joinedIndexMap,
                  List.of(leftInput, rightInput))
              .streamRoot(leftInput.streamRoot).nowFilter(leftInput.nowFilter).sort(leftInput.sort)
              .build());
        } else if (joinAnalysis.isA(Type.TEMPORAL)) {
          errors.fatal("Expected join condition to be equality condition on state's primary key.");
        }
      } else if (joinAnalysis.isA(Type.TEMPORAL)) {
        Side side = joinAnalysis.getOriginalSide();
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

    //Determine the joined primary key by removing pk columns that are constrained by the join condition
    Set<Integer> joinedPKIdx = new HashSet<>();
    List<PrimaryKeyMap.ColumnSet> combinedPkColumns = new ArrayList<>();
    combinedPkColumns.addAll(leftInputF.primaryKey.asList());
    combinedPkColumns.addAll(rightInputF.primaryKey.remap(remapRightSide).asList());
    Set<Integer> constrainedColumns = eqDecomp.getEqualities().stream().map(eq -> eq.isTwoSided()?eq.getRightIndex():eq.getOneSidedIndex())
            .collect(Collectors.toUnmodifiableSet());
    combinedPkColumns.removeIf(columnSet -> columnSet.containsAny(constrainedColumns));
    Side singletonSide = Side.NONE;
    for (Side side : new Side[]{Side.LEFT, Side.RIGHT}) {
      if (isPKConstrained.get(side)) singletonSide = side;
    }
    PrimaryKeyMap joinedPk = new PrimaryKeyMap(combinedPkColumns);


    //combine sorts if present
    SortOrder leftSort = leftInputF.sort;
    SortOrder rightSort = rightInputF.sort.remap(idx -> idx + leftSideMaxIdx);
    SortOrder joinedSort = joinAnalysis.isFlipped()? rightSort.join(leftSort):leftSort.join(rightSort);

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
      Optional<PhysicalRelationalTable> rootTable = Optional.empty();
      if (timePredicates.isEmpty() && leftInputF.streamRoot.filter(left -> rightInputF.streamRoot.filter(right -> right.equals(left)).isPresent()).isPresent()) {
        //Check that root primary keys are part of equality condition
        int numRootPks = leftInputF.streamRoot.get().getPrimaryKey().size();
        List<PrimaryKeyMap.ColumnSet> leftRootPks = leftInputF.primaryKey.asSubList(numRootPks);
        List<PrimaryKeyMap.ColumnSet> rightRootPks = rightInputF.primaryKey.asSubList(numRootPks)
                .stream().map(col -> col.remap(idx -> idx + leftSideMaxIdx)).collect(Collectors.toUnmodifiableList());
        boolean allRootsCovered = true;
        for (int i = 0; i < numRootPks; i++) {
          PrimaryKeyMap.ColumnSet left = leftRootPks.get(i), right = rightRootPks.get(i);
          if (eqDecomp.getEqualities().stream().noneMatch(eq -> left.contains(eq.getLeftIndex()) && right.contains(eq.getRightIndex()))) {
            allRootsCovered = false;
            break;
          }
        }
        if (allRootsCovered) {
          //Change primary key to only include root pk once and add equality time condition because timestamps must be equal
          TimePredicate eqCondition = new TimePredicate(
              rightInputF.timestamp.getAnyCandidate() + leftSideMaxIdx,
              leftInputF.timestamp.getAnyCandidate(), SqlKind.EQUALS, 0);
          timePredicates.add(eqCondition);
          conjunctions.add(eqCondition.createRexNode(rexUtil.getBuilder(), idxResolver, false));

          rootTable = leftInputF.streamRoot;
          //remove root pk columns from right side when combining primary keys
          PrimaryKeyMap.Builder concatPkBuilder = leftInputF.primaryKey.toBuilder();
          concatPkBuilder.addAll(rightInputF.primaryKey.asList().subList(numRootPks, rightInputF.primaryKey.getLength()).stream()
              .map(col -> col.remap(remapRightSide)).collect(Collectors.toUnmodifiableList()));
          joinedPk = concatPkBuilder.build();

        }
      }
      /*
      Check if timePredicates contains an equality or constrains both sides of the join
       */
      boolean isEquality = timePredicates.stream().anyMatch(TimePredicate::isEquality);
      boolean leftSideSmaller = timePredicates.stream().anyMatch(tp -> tp.getSmallerIndex() < leftSideMaxIdx);
      boolean rightSideSmaller = timePredicates.stream().anyMatch(tp -> tp.getSmallerIndex() >= leftSideMaxIdx);

      /*
      If we have time predicates that constraint timestamps from both sides of the join this is an interval join.
      We lock in the timestamps and pick the timestamp that is the upper bound as the resulting timestamp.
       */
      if (isEquality || (leftSideSmaller && rightSideSmaller)) {
        Set<Integer> timestampIndexes = timePredicates.stream()
            .flatMap(tp -> tp.getIndexes().stream()).collect(Collectors.toSet());
        errors.checkFatal(timestampIndexes.size() == 2, WRONG_INTERVAL_JOIN,
            "Invalid interval condition - more than 2 timestamp columns used: %s", condition);
//        errors.checkFatal(timePredicates.stream()
//                .filter(TimePredicate::isUpperBound).count() == 1, WRONG_INTERVAL_JOIN,
//            "Expected exactly one upper bound time predicate, but got: %s", condition);
        Timestamps joinTimestamp = Timestamps.build(isEquality?Timestamps.Type.OR: Timestamps.Type.AND)
            .candidates(timestampIndexes).build();;

        condition = RexUtil.composeConjunction(rexUtil.getBuilder(), conjunctions);
        joinAnalysis = joinAnalysis.makeA(Type.INTERVAL);
        relB.join(joinAnalysis.export(), condition); //Can treat as "standard" inner join since no modification is necessary in physical plan
        SqrlHintStrategyTable.INTERVAL_JOIN.addTo(relB);
        return setRelHolder(AnnotatedLP.build(relB.build(), TableType.STREAM,
            joinedPk, joinTimestamp, joinedIndexMap,
            List.of(leftInputF, rightInputF)).streamRoot(rootTable).sort(joinedSort).build());
      } else if (joinAnalysis.isA(Type.INTERVAL)) {
        errors.fatal("Interval joins require time bounds on the timestamp columns in the join condition.");
      }
    } else if (joinAnalysis.isA(Type.INTERVAL)) {
      errors.fatal(
          "Interval joins are only supported between two streams.");
    }

    //We detected no special time-based joins, so make it a generic join
    joinAnalysis = joinAnalysis.makeGeneric();
    relB.join(joinAnalysis.export(), condition);
    //Default joins without primary key constraints or interval bounds can be expensive, so we create a hint for the cost model
    new JoinCostHint(leftInputF.type, rightInputF.type, eqDecomp.getEqualities().size(), singletonSide).addTo(
        relB);

    //Determine timestamps for each side and add the max of those two as the resulting timestamp
    Timestamps.TimestampsBuilder joinTimestamp = Timestamps.build(Timestamps.Type.AND);
    //Add all candidates from both sides of the join
    joinTimestamp.candidates(leftInputF.timestamp.getCandidates());
    joinTimestamp.candidates(rightInputF.timestamp.getCandidates().stream().map(remapRightSide::map)
        .collect(Collectors.toUnmodifiableSet()));

    TableType resultType;
    Optional<PhysicalRelationalTable> rootTable = Optional.empty();
    if (rightInputF.type==TableType.STATIC) {
      resultType = leftInputF.type;
      rootTable = leftInputF.streamRoot;
    } else if (leftInputF.type==TableType.STATIC) {
      resultType = rightInputF.type;
      if (joinAnalysis.isA(Side.NONE)) rootTable = rightInputF.streamRoot;
    } else if (rightInputF.type.isStream() && leftInputF.type.isStream()) {
      resultType = TableType.STREAM;
    } else {
      resultType = TableType.STATE;
    }

    return setRelHolder(AnnotatedLP.build(relB.build(), resultType,
        joinedPk, joinTimestamp.build(), joinedIndexMap,
        List.of(leftInputF, rightInputF)).sort(joinedSort).streamRoot(rootTable).build());
  }

  @Override
  public RelNode visit(LogicalCorrelate logicalCorrelate) {
    AnnotatedLP leftInput = getRelHolder(logicalCorrelate.getLeft().accept(this))
        .inlineTopN(makeRelBuilder(), exec);
    AnnotatedLP rightInput = getRelHolder(logicalCorrelate.getRight().accept(this))
        .inlineTopN(makeRelBuilder(), exec);
    if (!leftInput.type.hasPrimaryKey()) {
      return processRelation(List.of(leftInput, rightInput), logicalCorrelate);
    }
    leftInput = leftInput.inlineTopN(makeRelBuilder(), exec);
    rightInput = rightInput.inlineAll(makeRelBuilder(), exec);
    final int leftSideMaxIdx = leftInput.getFieldLength();
    SelectIndexMap joinedIndexMap = leftInput.select.join(rightInput.select, leftSideMaxIdx, false);

    RelBuilder relB = makeRelBuilder();
    relB.push(leftInput.relNode);
    List<RexNode> requiredNodes = logicalCorrelate.getRequiredColumns().asList().stream().map(leftInput.select::map)
            .map(idx -> rexUtil.makeInputRef(idx, relB)).collect(Collectors.toList());
    relB.push(rightInput.relNode);
    relB.correlate(logicalCorrelate.getJoinType(), logicalCorrelate.getCorrelationId(), requiredNodes);
    PrimaryKeyMap.Builder pkBuilder = leftInput.getPrimaryKey().toBuilder();
    pkBuilder.addAll(rightInput.getPrimaryKey().remap(idx -> idx + leftSideMaxIdx).asList());
    return setRelHolder(AnnotatedLP.build(relB.build(), leftInput.getType(),
            pkBuilder.build(), leftInput.getTimestamp(), joinedIndexMap,
            List.of(leftInput, rightInput))
        .streamRoot(leftInput.streamRoot).nowFilter(leftInput.nowFilter).sort(leftInput.sort)
        .build());
  }

  private JoinModifier getJoinModifier(ImmutableList<RelHint> hints) {
    for (RelHint hint : hints) {
      if (hint.hintName.equals(JoinModifierHint.HINT_NAME)) {
        return JoinModifierHint.CONSTRUCTOR.fromHint(hint)
            .getJoinModifier();
      }
    }

    // Added during rule application (e.g. subquery removal)
    return JoinModifier.NONE;
  }

  @Override
  public RelNode visit(LogicalUnion logicalUnion) {
    List<AnnotatedLP> rawInputs = logicalUnion.getInputs().stream()
        .map(in -> getRelHolder(in.accept(this)).inlineNowFilter(makeRelBuilder(), exec).inlineTopN(makeRelBuilder(), exec))
            .collect(Collectors.toUnmodifiableList());
    Preconditions.checkArgument(rawInputs.size() > 0);
    if (!rawInputs.stream().allMatch(alp -> alp.type.isStream()) || !logicalUnion.all) {
      //Process as relation
      return processRelation(rawInputs, logicalUnion);
    }

    List<AnnotatedLP> inputs = rawInputs.stream().map(meta -> meta.copy().sort(SortOrder.EMPTY)
            .build()) //We ignore the sorts of the inputs (if any) since they are streams and we union them the default sort is timestamp
        .map(meta -> meta.postProcess(
            makeRelBuilder(), null, config, false, errors)) //The post-process makes sure the input relations are aligned and timestamps are chosen (pk,selects,timestamps)
        .collect(Collectors.toList());

    //Calcite ensures that a union has the same select columns. We need to also ensure primary keys and timestamps align.
    List<RelDataTypeField> fields = inputs.get(0).relNode.getRowType().getFieldList();
    PrimaryKeyMap pk = inputs.get(0).primaryKey;
    Preconditions.checkArgument(pk.isSimple(), "Primary key should be simple after post-processing");
    Timestamps timestamp = inputs.get(0).getTimestamp();
    SelectIndexMap select = inputs.get(0).select;
    Preconditions.checkArgument(timestamp.size()==1, "Should have only one timestamp after post-processing");
    if (!inputs.stream().allMatch(alp -> {
      errors.checkFatal(alp.select.equals(select),
              "Input streams select different columns");
      if (!alp.primaryKey.equals(pk)) {
        errors.warn("Union of streams with different primary keys (both column names and positions must align): %s vs %s",
            alp.getFieldNamesWithIndex(alp.primaryKey.asSimpleList()), inputs.get(0).getFieldNamesWithIndex(pk.asSimpleList()));
        return false;
      }
      if (!alp.timestamp.equals(timestamp)) {
        errors.warn("Union of streams with different timestamps (both column names and positions must align): %s vs %s",
            alp.getFieldNamesWithIndex(alp.timestamp.asList()), inputs.get(0).getFieldNamesWithIndex(timestamp.asList()));
        return false;
      }
      List<RelDataTypeField> alpFields = alp.relNode.getRowType().getFieldList();
      for (Integer pkIndex : pk.asSimpleList()) {
        if (!fields.get(pkIndex).equals(alpFields.get(pkIndex))) {
          errors.warn("Union of streams with different primary key column types: %s vs %s",
              fields.get(pkIndex), alpFields.get(pkIndex));
          return false;
        }
      }
      return true;
    })) {
      return processRelation(rawInputs, logicalUnion);
    }

    List<Integer> selectIndexes = new ArrayList<>();
    selectIndexes.addAll(select.targetsAsList());
    List<String> selectNames = new ArrayList<>();
    IntStream.range(0,selectIndexes.size()).forEach(i -> selectNames.add(i,null)); //those names must be the same, just copy them
    //Add names for any added columns
    Stream.concat(IntStream.range(0, pk.getLength()).mapToObj(pkNo -> Pair.of(ReservedName.SYSTEM_PRIMARY_KEY.suffix(String.valueOf(pkNo)), pk.get(pkNo).getOnly())),
                    timestamp.getCandidates().stream().map(idx -> Pair.of(ReservedName.SYSTEM_TIMESTAMP, idx))).
            filter(pair -> !selectIndexes.contains(pair.getRight()))
            .sorted(Comparator.comparing(Pair::getRight))
            .forEach(pair -> {selectIndexes.add(pair.getRight()); selectNames.add(pair.getLeft().getDisplay());});

    //In the following, we can assume that the input relations are aligned because we post-processed them
    RelBuilder relBuilder = makeRelBuilder();

    for (int i = 0; i < inputs.size(); i++) {
      AnnotatedLP input = inputs.get(i);
      relBuilder.push(input.relNode);
      CalciteUtil.addProjection(relBuilder, selectIndexes, selectNames);
    }
    relBuilder.union(true,inputs.size());
    return setRelHolder(
        AnnotatedLP.build(relBuilder.build(), TableType.STREAM, pk, timestamp, select, inputs)
            .streamRoot(Optional.empty()).build());
  }

  @Override
  public RelNode visit(LogicalAggregate aggregate) {
    //Need to inline TopN before we aggregate, but we postpone inlining now-filter in case we can push it through
    final AnnotatedLP input = getRelHolder(aggregate.getInput().accept(this))
            .inlineTopN(makeRelBuilder(), exec);
    if (isRelation(input) || aggregate.groupSets.size()>1) {
      return processRelation(List.of(input), aggregate);
    }
    return setRelHolder(processAggregate(input, aggregate));
  }

  public AnnotatedLP processAggregate(AnnotatedLP input, LogicalAggregate aggregate) {
    final List<Integer> groupByIdx = aggregate.getGroupSet().asList().stream()
        .map(input.select::map)
        .collect(Collectors.toList());
    List<AggregateCall> aggregateCalls = mapAggregateCalls(aggregate, input);
    int targetLength = groupByIdx.size() + aggregateCalls.size();

    exec.requireAggregates(aggregateCalls);

    //Check if this a nested aggregation of a stream on root primary key during write stage
    if (input.type == TableType.STREAM && input.streamRoot.isPresent()) {
      PhysicalRelationalTable rootTable = input.streamRoot.get();
      int numRootPks = rootTable.getPrimaryKey().size();
      //Check that all root primary keys are part of the groupBy
      if(input.primaryKey.asList().subList(0, numRootPks).stream().allMatch(col -> col.containsAny(groupByIdx))) {
        return handleNestedAggregationInStream(input, groupByIdx, aggregateCalls);
      }
    }


    //Check if this is a time-window aggregation
    if (input.type == TableType.STREAM) {
      Optional<Timestamps.TimeWindow> timeWindow = findTimeWindowInGroupBy(groupByIdx, input.timestamp, input.relNode);
      if (timeWindow.isPresent()) {
        return handleTimeWindowAggregation(input, groupByIdx, aggregateCalls, targetLength,
                timeWindow.get());
      }
    }

    /* Check if any of the aggregate calls is a max(timestamp candidate) in which
       case that candidate should be the fixed timestamp and the result of the agg should be
       the resulting timestamp
     */
    Optional<MaxTimestamp> maxTimestamp = TimestampAnalysis.findMaxTimestamp(
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
    //Find candidate that's in the groupBy, else use the best option
    RelBuilder relB = makeRelBuilder();
    relB.push(input.relNode);
    int timestampIdx = -1;
    for (int cand : input.timestamp.getCandidates()) {
      if (groupByIdx.contains(cand)) {
        timestampIdx = cand;
        break;
      }
    }
    if (timestampIdx<0) timestampIdx=input.timestamp.getBestCandidate(relB);

    Pair<PkAndSelect, Timestamps> addedTimestamp =
        addTimestampAggregate(relB, groupByIdx, timestampIdx, aggregateCalls);
    PkAndSelect pkSelect = addedTimestamp.getKey();
    Timestamps timestamp = addedTimestamp.getValue();

    TumbleAggregationHint.instantOf(timestampIdx).addTo(relB);

    NowFilter nowFilter = input.nowFilter.remap(IndexMap.singleton(timestampIdx,
        timestamp.getOnlyCandidate()));

    return AnnotatedLP.build(relB.build(), TableType.STREAM, pkSelect.pk, timestamp, pkSelect.select,
                input)
            .streamRoot(input.streamRoot)
            .nowFilter(nowFilter).build();
  }

  private Optional<Timestamps.TimeWindow> findTimeWindowInGroupBy(
      List<Integer> groupByIdx, Timestamps timestamp, RelNode input) {
    //Determine if one of the groupBy keys is a timestamp or time window
    List<Timestamps.TimeWindow> windows = timestamp.getWindows().stream().filter(w -> w.qualifiesWindow(groupByIdx))
            .collect(Collectors.toUnmodifiableList());
    if (windows.size() > 1) {
      errors.fatal(ErrorCode.NOT_YET_IMPLEMENTED, "Do not currently support grouping by "
              + "multiple time windows: [%s]", windows);
    } else if (windows.size()==1) {
      return Optional.of(windows.get(0));
    }
    return Optional.empty();
  }

  private AnnotatedLP handleTimeWindowAggregation(AnnotatedLP input,
      List<Integer> groupByIdx, List<AggregateCall> aggregateCalls, int targetLength,
                                                  Timestamps.TimeWindow window) {
    Preconditions.checkArgument(window instanceof Timestamps.SimpleTumbleWindow,
            "Expected simple tumble window");
    Timestamps.SimpleTumbleWindow simpleWindow = (Timestamps.SimpleTumbleWindow)window;
    //This must exist otherwise the window would not have matched
    int keyIdx = IntStream.range(0, groupByIdx.size())
            .filter(idx -> groupByIdx.get(idx)==simpleWindow.getWindowIndex()).findAny().getAsInt();

    //Fix timestamp (if not already fixed)
    Timestamps newTimestamp = Timestamps.ofFixed(keyIdx);
    //Now filters must be on the timestamp - otherwise we need to inline them
    NowFilter nowFilter = NowFilter.EMPTY;
    if (!input.nowFilter.isEmpty()) {
      if (input.nowFilter.getTimestampIndex()!=simpleWindow.getTimestampIndex()) {
        input = input.inlineNowFilter(makeRelBuilder(), exec);
      } else {
        long intervalExpansion = simpleWindow.getWindowWidthMillis();
        //Update new Filter with expansion based on time window
        nowFilter = input.nowFilter.map(tp -> new TimePredicate(tp.getSmallerIndex(),
                keyIdx, tp.getComparison(), tp.getIntervalLength() + intervalExpansion));
      }
    }

    RelBuilder relB = makeRelBuilder();
    relB.push(input.relNode);
    relB.aggregate(relB.groupKey(Ints.toArray(groupByIdx)), aggregateCalls);
    TumbleAggregationHint.functionOf(simpleWindow.getWindowIndex(),
            simpleWindow.getTimestampIndex(),
        simpleWindow.getWindowWidthMillis(), simpleWindow.getWindowOffsetMillis()).addTo(relB);
    PkAndSelect pkSelect = aggregatePkAndSelect(groupByIdx, targetLength);

    /* TODO: this type of streaming aggregation requires a post-filter in the database (in physical model) to filter out "open" time buckets,
    i.e. time_bucket_col < time_bucket_function(now()) [if now() lands in a time bucket, that bucket is still open and shouldn't be shown]
      set to "SHOULD" once this is supported
     */

    return AnnotatedLP.build(relB.build(), TableType.STREAM, pkSelect.pk, newTimestamp,
                pkSelect.select, input)
            .nowFilter(nowFilter).build();
  }

  private AnnotatedLP handleTimestampedAggregationInStream(LogicalAggregate aggregate, AnnotatedLP input,
      List<Integer> groupByIdx, List<AggregateCall> aggregateCalls,
      Optional<MaxTimestamp> maxTimestamp, int targetLength) {

    RelBuilder relB = makeRelBuilder();
    relB.push(input.relNode);
    //Fix best timestamp (if not already fixed)
    Timestamps inputTimestamp = input.timestamp;
    int timestampIdx = maxTimestamp.map(MaxTimestamp::getTimestampIdx)
        .orElse(inputTimestamp.getBestCandidate(relB));

    if (!input.nowFilter.isEmpty() && exec.supportsFeature(EngineFeature.STREAM_WINDOW_AGGREGATION)) {
      NowFilter nowFilter = input.nowFilter;
      //Determine timestamp, add to group-By and
      Preconditions.checkArgument(nowFilter.getTimestampIndex() == timestampIdx,
          "Timestamp indexes don't match");
      errors.checkFatal(!groupByIdx.contains(timestampIdx),
          "Cannot group on timestamp: %s", rexUtil.getFieldName(timestampIdx, input.relNode));

      Pair<PkAndSelect, Timestamps> addedTimestamp =
          addTimestampAggregate(relB, groupByIdx, timestampIdx, aggregateCalls);
      PkAndSelect pkAndSelect = addedTimestamp.getKey();
      Timestamps timestamp = addedTimestamp.getValue();

      //Convert now-filter to sliding window and add as hint
      long intervalWidthMs = nowFilter.getPredicate().getIntervalLength();
      // TODO: extract slide-width from hint
      long slideWidthMs = intervalWidthMs / config.getSlideWindowPanes();
      errors.checkFatal(slideWidthMs > 0 && slideWidthMs < intervalWidthMs,
          "Invalid sliding window widths: %s - %s", intervalWidthMs, slideWidthMs);
      new SlidingAggregationHint(timestampIdx, intervalWidthMs, slideWidthMs).addTo(relB);

      TopNConstraint dedup = TopNConstraint.makeDeduplication(pkAndSelect.pk.asSimpleList(),
          timestamp.getOnlyCandidate());
      return AnnotatedLP.build(relB.build(), TableType.VERSIONED_STATE, pkAndSelect.pk,
              timestamp, pkAndSelect.select, input)
          .topN(dedup).build();
    } else {
      return handleStandardAggregation(input, groupByIdx, aggregateCalls, maxTimestamp, targetLength);
    }
  }

  private AnnotatedLP handleStandardAggregation(AnnotatedLP input,
      List<Integer> groupByIdx, List<AggregateCall> aggregateCalls,
      Optional<MaxTimestamp> maxTimestamp, int targetLength) {
    //Standard aggregation produces a state table
    input = input.inlineNowFilter(makeRelBuilder(), exec);
    RelBuilder relB = makeRelBuilder();
    relB.push(input.relNode);
    Timestamps resultTimestamp;
    int selectLength = targetLength;
    TableType resultType = TableType.STATE;
    if (maxTimestamp.isPresent()) {
      MaxTimestamp mt = maxTimestamp.get();
      resultTimestamp = Timestamps.ofFixed(groupByIdx.size() + mt.getAggCallIdx());
    } else if (input.timestamp.hasCandidates()) { //add max timestamp
      int bestTimestampIdx = input.timestamp.getBestCandidate(relB);
      //New timestamp column is added at the end
      resultTimestamp = Timestamps.ofFixed(targetLength);
      AggregateCall maxTimestampAgg = rexUtil.makeMaxAggCall(bestTimestampIdx,
          ReservedName.SYSTEM_TIMESTAMP.getCanonical(), groupByIdx.size(), relB.peek());
      aggregateCalls.add(maxTimestampAgg);
      targetLength++;
    } else {
      Preconditions.checkArgument(input.type == TableType.STATIC);
      resultTimestamp = Timestamps.UNDEFINED;
      resultType = TableType.STATIC;
    }


    relB.aggregate(relB.groupKey(Ints.toArray(groupByIdx)), aggregateCalls);
    PkAndSelect pkSelect = aggregatePkAndSelect(groupByIdx, selectLength);
    return AnnotatedLP.build(relB.build(), resultType, pkSelect.pk,
        resultTimestamp, pkSelect.select, input).build();
  }

  private Pair<PkAndSelect, Timestamps> addTimestampAggregate(
      RelBuilder relBuilder, List<Integer> groupByIdx, int timestampIdx,
      List<AggregateCall> aggregateCalls) {
    int targetLength = groupByIdx.size() + aggregateCalls.size();
    //if agg-calls return types are nullable because there are no group-by keys, we have to make then non-null
    if (groupByIdx.isEmpty()) {
      aggregateCalls = aggregateCalls.stream().map(agg -> CalciteUtil.makeNotNull(agg,
          relBuilder.getTypeFactory())).collect(Collectors.toList());
    }

    List<Integer> groupByIdxTimestamp = new ArrayList<>(groupByIdx);
    boolean addedTimestamp = !groupByIdxTimestamp.contains(timestampIdx);
    if (addedTimestamp) {
      groupByIdxTimestamp.add(timestampIdx);
      targetLength++;
    }
    Collections.sort(groupByIdxTimestamp);
    int newTimestampIdx = groupByIdxTimestamp.indexOf(timestampIdx);
    Timestamps timestamp = Timestamps.ofFixed(newTimestampIdx);

    relBuilder.aggregate(relBuilder.groupKey(Ints.toArray(groupByIdxTimestamp)), aggregateCalls);
    //Restore original order of groupByIdx in primary key and select
    PkAndSelect pkAndSelect =
        aggregatePkAndSelect(groupByIdx, groupByIdxTimestamp, targetLength);
    return Pair.of(pkAndSelect, timestamp);
  }

  private PkAndSelect aggregatePkAndSelect(List<Integer> originalGroupByIdx,
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
  private PkAndSelect aggregatePkAndSelect(List<Integer> originalGroupByIdx,
      List<Integer> finalGroupByIdx, int targetLength) {
    Preconditions.checkArgument(
        finalGroupByIdx.equals(finalGroupByIdx.stream().sorted().collect(Collectors.toList())),
        "Expected final groupByIdx to be sorted");
    Preconditions.checkArgument(originalGroupByIdx.stream().map(finalGroupByIdx::indexOf).allMatch(idx ->idx >= 0),
        "Invalid groupByIdx [%s] to [%s]", originalGroupByIdx, finalGroupByIdx);
    PrimaryKeyMap pk = PrimaryKeyMap.of(originalGroupByIdx.stream().map(finalGroupByIdx::indexOf).collect(
            Collectors.toList()));
    assert pk.getLength() == originalGroupByIdx.size() && pk.isSimple();

    ContiguousSet<Integer> additionalIdx = ContiguousSet.closedOpen(finalGroupByIdx.size(), targetLength);
    SelectIndexMap.Builder selectBuilder = SelectIndexMap.builder(pk.getLength() + additionalIdx.size());
    selectBuilder.addAll(pk.asSimpleList()).addAll(additionalIdx);
    return new PkAndSelect(pk, selectBuilder.build(targetLength));
  }

  @Value
  private static class PkAndSelect {

    PrimaryKeyMap pk;
    SelectIndexMap select;
  }

  @Override
  public RelNode visit(LogicalSort logicalSort) {
    AnnotatedLP input = getRelHolder(logicalSort.getInput().accept(this));
    if (isRelation(input) || logicalSort.offset!=null) {
      return processRelation(List.of(input), logicalSort);
    }

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
      Optional<Integer> timestampOrder = LPConverterUtil.getTimestampOrderIndex(newCollation, input.timestamp);
      TopNConstraint topN = new TopNConstraint(List.of(), false, newCollation, limit,
          timestampOrder.isPresent());
      if (limit.get()==1) {
        //Treat this as a dedup with empty primary key if limit is 1 and timestamp order
        Timestamps timestamp = input.timestamp;
        if (timestampOrder.isPresent()) timestamp = Timestamps.ofFixed(timestampOrder.get());
        result = input.copy().primaryKey(PrimaryKeyMap.none())
            .timestamp(timestamp).topN(topN).build();
      } else {
        result = input.copy()
            .topN(topN).build();
      }
    } else {
      //We can just replace any old order that might be present
      result = input.copy().sort(new SortOrder(newCollation)).build();
    }
    return setRelHolder(result);
  }

}
