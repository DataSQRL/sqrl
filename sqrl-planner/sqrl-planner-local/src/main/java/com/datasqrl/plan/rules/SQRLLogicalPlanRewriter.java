/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.rules;

import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.engine.EngineCapability;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.function.SqrlTimeTumbleFunction;
import com.datasqrl.plan.hints.*;
import com.datasqrl.plan.local.generate.QueryTableFunction;
import com.datasqrl.plan.rules.JoinAnalysis.Side;
import com.datasqrl.plan.rules.JoinAnalysis.Type;
import com.datasqrl.plan.rules.SQRLConverter.Config;
import com.datasqrl.plan.table.*;
import com.datasqrl.plan.util.*;
import com.datasqrl.plan.util.TimestampAnalysis.MaxTimestamp;
import com.datasqrl.schema.NestedRelationship;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.SqrlRexUtil;
import com.datasqrl.util.SqrlRexUtil.JoinConditionDecomposition;
import com.datasqrl.util.SqrlRexUtil.JoinConditionDecomposition.EqualityCondition;
import com.google.common.base.Preconditions;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.JoinModifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.datasqrl.error.ErrorCode.*;

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
    Preconditions.checkArgument(!(relHolder.type.hasTimestamp() && relHolder.timestamp.is(
        Timestamps.Type.UNDEFINED)),"Timestamp required");
    Preconditions.checkArgument(!(relHolder.type.hasPrimaryKey() && relHolder.primaryKey.isUndefined()),"Primary key required");
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
    if (exec.isMaterialize(table) && topN.isDeduplication()) {
      // We can drop topN since that gets enforced by writing to DB with primary key
      topN = TopNConstraint.EMPTY;
    }
    PrimaryKeyMap pkMap = PrimaryKeyMap.UNDEFINED;
    Optional<PhysicalRelationalTable> rootTable = Optional.empty();
    if (table.getPrimaryKey().isDefined()) {
      rootTable = Optional.of(table);
      pkMap = PrimaryKeyMap.of(table.getPrimaryKey().asArray());
    }
    if (CalciteUtil.hasNestedTable(table.getRowType())) {
      exec.require(EngineCapability.DENORMALIZE);
    };
    return new AnnotatedLP(relNode, table.getType(),
        pkMap,
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
      return setRelHolder(new AnnotatedLP(functionScan, TableType.NESTED,
          PrimaryKeyMap.of(nestedRel.getLocalPKs()),
          Timestamps.UNDEFINED,
          SelectIndexMap.identity(numColumns, numColumns),
          Optional.empty(),
          NowFilter.EMPTY, TopNConstraint.EMPTY,
          SortOrder.EMPTY, List.of()));
    } else if (tableFunction instanceof QueryTableFunction) {
      exec.require(EngineCapability.TABLE_FUNCTION_SCAN); //We only support table functions on the read side
      QueryTableFunction tblFct = (QueryTableFunction) tableFunction;
      QueryRelationalTable queryTable = tblFct.getQueryTable();
      return setRelHolder(createAnnotatedRootTable(functionScan, queryTable));
    } else {
      throw errors.exception("Invalid table function encountered in query: %s [%s]", functionScan, tableFunction.getClass());
    }
  }

  @Override
  public RelNode visit(LogicalValues logicalValues) {
    //TODO
    return null;
  }

  private static final SqrlRexUtil.RexFinder FIND_NOW = SqrlRexUtil.findFunction(SqrlRexUtil::isNOW);

  @Override
  public RelNode visit(LogicalFilter logicalFilter) {
    AnnotatedLP input = getRelHolder(logicalFilter.getInput().accept(this));
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
        Timestamps timestamp = baseInput.timestamp;
        RelBuilder relB = makeRelBuilder();
        relB.push(baseInput.relNode);
        boolean isDistinct = false;
        if (topNHint.getType() == TopNHint.Type.SELECT_DISTINCT) {
          List<Integer> distincts = SqrlRexUtil.combineIndexes(partition,
              trivialMap.targetsAsList());
          pk = PrimaryKeyMap.of(distincts);
          if (partition.isEmpty()) {
            //If there is no partition, we can ignore the sort order plus limit and turn this into a simple deduplication
            partition = pk.asList();
            if (timestamp.hasCandidates()) {
              timestamp = Timestamps.ofFixed(timestamp.getBestCandidate(relB));
              collation = LPConverterUtil.getTimestampCollation(timestamp.getOnlyCandidate());
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
          errors.checkFatal(LPConverterUtil.getTimestampOrderIndex(collation, timestamp)
                  .filter(timestamp.isCandidatePredicate()).isPresent(),
              DISTINCT_ON_TIMESTAMP, "Not a valid timestamp order in ORDER BY clause: %s",
              rexUtil.getCollationName(collation, baseInput.relNode));
          errors.checkFatal(baseInput.type.isStream(), WRONG_TABLE_TYPE,
              "DISTINCT ON statements require stream table as input");

          pk = PrimaryKeyMap.of(partition);
          select = SelectIndexMap.identity(targetLength, targetLength); //Select everything
          //Extract timestamp from collation
          Integer timestampIdx = LPConverterUtil.getTimestampOrderIndex(
              collation, timestamp).get();
          collation = LPConverterUtil.getTimestampCollation(timestampIdx);
          timestamp = Timestamps.ofFixed(timestampIdx);
          limit = Optional.of(1); //distinct on has implicit limit of 1
        } else if (topNHint.getType() == TopNHint.Type.TOP_N) {
          //Prepend partition to primary key
          List<Integer> pkIdx = SqrlRexUtil.combineIndexes(partition, pk.asList());
          pk = PrimaryKeyMap.of(pkIdx);
        }

        TopNConstraint topN = new TopNConstraint(partition, isDistinct, collation, limit,
            LPConverterUtil.getTimestampOrderIndex(collation, timestamp).isPresent());
        TableType resultType = TableType.STATE;
        if (baseInput.type.isStream() && topN.isDeduplication()) resultType = TableType.VERSIONED_STATE;
        return setRelHolder(baseInput.copy().relNode(relB.build()).type(resultType)
            .primaryKey(pk).select(select).timestamp(timestamp)
            .topN(topN).sort(SortOrder.EMPTY).build());
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
    NowFilter nowFilter = NowFilter.EMPTY;
    Timestamps.TimestampsBuilder timestampBuilder = Timestamps.build(input.timestamp.getType());
    for (Ord<RexNode> exp : Ord.<RexNode>zip(logicalProject.getProjects())) {
      RexNode mapRex = input.select.map(exp.e, input.relNode.getRowType());
      updatedProjects.add(exp.i, mapRex);
      updatedNames.add(exp.i, logicalProject.getRowType().getFieldNames().get(exp.i));
      Optional<Integer> inputRef = CalciteUtil.getInputRefRobust(mapRex);
      Optional<Integer> preservedTimestamp;
      if (inputRef.isPresent()) { //Direct mapping
        int originalIndex = inputRef.get();
        if (input.timestamp.isCandidate(originalIndex)) timestampBuilder.index(exp.i);
        if (mappedProjects.putIfAbsent(originalIndex, exp.i) != null) {
          //We are ignoring this pk mapping because the prior one takes precedence, let's see if we should warn the user
          if (input.primaryKey.containsIndex(originalIndex)) {
            errors.warn(MULTIPLE_PRIMARY_KEY, "The primary key is mapped to multiple columns in SELECT: %s", logicalProject.getProjects());
          }
        }
      } else if ((preservedTimestamp = TimestampAnalysis.getPreservedTimestamp(
          mapRex, input.timestamp)).isPresent()) { //Check for preserved timestamps through certain function calls
        //1. Timestamps: Timestamps are preserved if they are mapped through timestamp-preserving functions
        timestampBuilder.index(exp.i);
        //See if we can preserve the now-filter as well or need to inline it
        int originalIndex = preservedTimestamp.get();
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
    //Make sure we pull the primary keys through (i.e. append those to the projects if not already present)
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
    RelBuilder relB = makeRelBuilder();
    relB.push(input.relNode);
    //Make sure we are pulling at least one timestamp candidates through
    Timestamps timestamp = timestampBuilder.build();
    if (!timestamp.hasCandidates()) {
      int oldTimestampIdx = input.timestamp.getBestCandidate(relB);
      Preconditions.checkArgument(!mappedProjects.containsKey(oldTimestampIdx));
      int target = updatedProjects.size();
      updatedProjects.add(target,
          RexInputRef.of(oldTimestampIdx, input.relNode.getRowType()));
      updatedNames.add(null);
      mappedProjects.put(oldTimestampIdx, target);
      timestampBuilder.index(target);
      timestamp = timestampBuilder.build();
    }
    //Update now-filter because timestamp are updated
    if (!input.nowFilter.isEmpty()) {
      nowFilter = input.nowFilter.remap(IndexMap.of(mappedProjects));
    }
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
    relB.project(updatedProjects, updatedNames);
    RelNode newProject = relB.build();
    int fieldCount = updatedProjects.size();
    exec.requireRex(updatedProjects);
    return setRelHolder(AnnotatedLP.build(newProject, input.type, primaryKey.build(),
            timestamp, SelectIndexMap.identity(logicalProject.getProjects().size(), fieldCount),
            input)
        .rootTable(input.rootTable).nowFilter(nowFilter).sort(sort).build());
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
    JoinAnalysis joinAnalysis = JoinAnalysis.of(logicalJoin.getJoinType(),
        getJoinModifier(logicalJoin.getHints()));

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
          if (rightInput.type!=TableType.VERSIONED_STATE && !exec.supports(EngineCapability.TEMPORAL_JOIN_ON_STATE)) {
            errors.checkFatal(false, ErrorCode.NOT_YET_IMPLEMENTED,
                "Currently temporal joins are limited to versioned states.");
          }

          Timestamps joinTimestamp = leftInput.timestamp.asBest(relB);
          relB.push(rightInput.relNode);

          PrimaryKeyMap pk = leftInput.primaryKey.toBuilder().build();
          TemporalJoinHint hint = new TemporalJoinHint(
              joinTimestamp.getOnlyCandidate(),
              rightInput.timestamp.getOnlyCandidate(),
              rightInput.primaryKey.asArray());
          joinAnalysis = joinAnalysis.makeA(Type.TEMPORAL);
          relB.join(joinAnalysis.export(), condition);
          hint.addTo(relB);
          exec.require(EngineCapability.TEMPORAL_JOIN);
          return setRelHolder(AnnotatedLP.build(relB.build(), TableType.STREAM,
                  pk, joinTimestamp, joinedIndexMap,
                  List.of(leftInput, rightInput))
              .rootTable(leftInput.rootTable).nowFilter(leftInput.nowFilter).sort(leftInput.sort)
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
      Optional<PhysicalRelationalTable> rootTable = Optional.empty();
      if (timePredicates.isEmpty() && leftInputF.rootTable.filter(left -> rightInputF.rootTable.filter(right -> right.equals(left)).isPresent()).isPresent()) {
        //Check that root primary keys are part of equality condition
        List<EqualityCondition> rootPkPairs = new ArrayList<>();
        int numRootPks = leftInputF.rootTable.get().getPrimaryKey().size();
        for (int i = 0; i < numRootPks; i++) {
          rootPkPairs.add(new EqualityCondition(leftInputF.primaryKey.map(i),
              rightInputF.primaryKey.map(i) + leftSideMaxIdx));
        }
        if (eqDecomp.getEqualities().containsAll(rootPkPairs)) {
          //Change primary key to only include root pk once and add equality time condition because timestamps must be equal
          TimePredicate eqCondition = new TimePredicate(
              rightInputF.timestamp.getAnyCandidate() + leftSideMaxIdx,
              leftInputF.timestamp.getAnyCandidate(), SqlKind.EQUALS, 0);
          timePredicates.add(eqCondition);
          conjunctions.add(eqCondition.createRexNode(rexUtil.getBuilder(), idxResolver, false));

          rootTable = leftInputF.rootTable;
          //remove root pk columns from right side when combining primary keys
          concatPkBuilder = leftInputF.primaryKey.toBuilder();
          List<Integer> rightPks = rightInputF.primaryKey.asList();
          rightPks.subList(numRootPks, rightPks.size()).stream()
              .map(idx -> idx + leftSideMaxIdx).forEach(concatPkBuilder::pkIndex);
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
        errors.checkFatal(timePredicates.stream()
                .filter(TimePredicate::isUpperBound).count() == 1, WRONG_INTERVAL_JOIN,
            "Expected exactly one upper bound time predicate, but got: %s", condition);
        Timestamps joinTimestamp = Timestamps.build(isEquality?Timestamps.Type.OR: Timestamps.Type.AND)
            .indexes(timestampIndexes).build();;

        condition = RexUtil.composeConjunction(rexUtil.getBuilder(), conjunctions);
        joinAnalysis = joinAnalysis.makeA(Type.INTERVAL);
        relB.join(joinAnalysis.export(), condition); //Can treat as "standard" inner join since no modification is necessary in physical plan
        SqrlHintStrategyTable.INTERVAL_JOIN.addTo(relB);
        return setRelHolder(AnnotatedLP.build(relB.build(), TableType.STREAM,
            joinedPk, joinTimestamp, joinedIndexMap,
            List.of(leftInputF, rightInputF)).rootTable(rootTable).sort(joinedSort).build());
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
    if (leftInputF.timestamp.is(Timestamps.Type.AND)) {
      joinTimestamp.indexes(leftInputF.timestamp.getIndexes());
    } else {
      joinTimestamp.index(leftInputF.timestamp.getAnyCandidate());
    }
    if (rightInputF.timestamp.is(Timestamps.Type.AND)) {
      joinTimestamp.indexes(rightInputF.timestamp.getIndexes());
    } else {
      joinTimestamp.index(rightInputF.timestamp.getAnyCandidate());
    }

    TableType resultType = leftInputF.type.isStream() && rightInputF.type.isStream()?TableType.STREAM:TableType.STATE;

    return setRelHolder(AnnotatedLP.build(relB.build(), resultType,
        joinedPk, joinTimestamp.build(), joinedIndexMap,
        List.of(leftInputF, rightInputF)).sort(joinedSort).build());
  }

  @Override
  public RelNode visit(LogicalCorrelate logicalCorrelate) {
    AnnotatedLP leftInput = getRelHolder(logicalCorrelate.getLeft().accept(this))
        .inlineTopN(makeRelBuilder(), exec);
    AnnotatedLP rightInput = getRelHolder(logicalCorrelate.getRight().accept(this))
        .inlineTopN(makeRelBuilder(), exec);

    final int leftSideMaxIdx = leftInput.getFieldLength();
    SelectIndexMap joinedIndexMap = leftInput.select.join(rightInput.select, leftSideMaxIdx, false);

    if (rightInput.type==TableType.NESTED) {
      RelBuilder relB = makeRelBuilder();
      relB.push(leftInput.relNode);
      int requiredColumn = leftInput.select.map(
          Iterables.getOnlyElement(logicalCorrelate.getRequiredColumns().asList()));
      RexNode requiredNode = rexUtil.makeInputRef(requiredColumn, relB);
      Preconditions.checkArgument(CalciteUtil.isNestedTable(requiredNode.getType()));
      relB.push(rightInput.relNode);
      relB.correlate(logicalCorrelate.getJoinType(), logicalCorrelate.getCorrelationId(), requiredNode);
      PrimaryKeyMap.PrimaryKeyMapBuilder pkBuilder = leftInput.getPrimaryKey().toBuilder();
      rightInput.getPrimaryKey().remap(idx -> idx + leftSideMaxIdx).asList().forEach(pkBuilder::pkIndex);
      return setRelHolder(AnnotatedLP.build(relB.build(), leftInput.getType(),
              pkBuilder.build(), leftInput.getTimestamp(), joinedIndexMap,
              List.of(leftInput, rightInput))
          .rootTable(leftInput.rootTable).nowFilter(leftInput.nowFilter).sort(leftInput.sort)
          .build());
    }

    leftInput = leftInput.inlineNowFilter(makeRelBuilder(), exec);
    rightInput = rightInput.inlineNowFilter(makeRelBuilder(), exec);
    throw new UnsupportedOperationException("Correlate not yet supported");
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
    errors.checkFatal(logicalUnion.all, NOT_YET_IMPLEMENTED,
        "Currently, only UNION ALL is supported. Combine with SELECT DISTINCT for UNION");

    List<AnnotatedLP> inputs = logicalUnion.getInputs().stream()
        .map(in -> getRelHolder(in.accept(this)).inlineTopN(makeRelBuilder(), exec))
        .map(meta -> meta.copy().sort(SortOrder.EMPTY)
            .build()) //We ignore the sorts of the inputs (if any) since they are streams and we union them the default sort is timestamp
        .map(meta -> meta.postProcess(
            makeRelBuilder(), null, exec, errors)) //The post-process makes sure the input relations are aligned and timestamps are chosen (pk,selects,timestamps)
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
    Set<Integer> timestampIndexes = inputs.get(0).timestamp.asList().stream()
        .filter(idx -> idx < maxSelectIdx).collect(Collectors.toSet());
    for (AnnotatedLP input : inputs) {
      errors.checkFatal(select.equals(input.select),
          "Input streams select different columns");
      //Validate primary key and selects line up between the streams
      errors.checkFatal(selectIndexes.containsAll(input.primaryKey.asList()),
          "The tables in the union have different primary keys. UNION requires uniform primary keys.");
      timestampIndexes.retainAll(input.timestamp.asList());
    }

    List<Integer> unionPk = new ArrayList<>();
    for (int i = 0; i < inputs.size(); i++) {
      AnnotatedLP input = inputs.get(i);
      TimestampInference localTimestamp;
      List<Integer> localSelectIndexes = new ArrayList<>(selectIndexes);
      List<String> localSelectNames = new ArrayList<>(selectNames);
      if (timestampIndexes.isEmpty()) { //Pick best and append
        Integer timestampIdx = input.timestamp.getAnyCandidate();
        localSelectIndexes.add(timestampIdx);
        localSelectNames.add(ReservedName.SYSTEM_TIMESTAMP.getCanonical());
      }
      input.primaryKey.asList().stream().filter(idx -> !unionPk.contains(idx)).forEach(unionPk::add);

      relBuilder.push(input.relNode);
      CalciteUtil.addProjection(relBuilder, localSelectIndexes, localSelectNames);
    }
    timestampIndexes = Set.of(maxSelectIdx); //Timestamp was appended at the end
    Timestamps timestamp = Timestamps.build(Timestamps.Type.OR).indexes(timestampIndexes).build();
    relBuilder.union(true, inputs.size());
    PrimaryKeyMap newPk = PrimaryKeyMap.of(unionPk);
    if (!isStream) exec.require(EngineCapability.UNION_STATE);
    return setRelHolder(
        AnnotatedLP.build(relBuilder.build(), isStream?TableType.STREAM:TableType.STATE, newPk, timestamp, select, inputs)
            .rootTable(Optional.empty()).build());
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
    if (input.type == TableType.STREAM && input.rootTable.isPresent()) {
      PhysicalRelationalTable rootTable = input.rootTable.get();
      int numRootPks = rootTable.getPrimaryKey().size();
      if(numRootPks <= groupByIdx.size()
          && groupByIdx.subList(0, numRootPks)
          .equals(input.primaryKey.asList().subList(0, numRootPks))) {
        return handleNestedAggregationInStream(input, groupByIdx, aggregateCalls);
      }
    }


    //Check if this is a time-window aggregation (i.e. a roll-up)
    Pair<Integer, Integer> timestampAndGroupKey;
    if (input.type == TableType.STREAM && input.getRelNode() instanceof LogicalProject
        && (timestampAndGroupKey = findTimestampInGroupBy(groupByIdx, input.timestamp, input.relNode))!=null) {
      return handleTimeWindowAggregation(input, groupByIdx, aggregateCalls, targetLength,
          timestampAndGroupKey.getLeft(), timestampAndGroupKey.getRight());
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
    //Find candidate that's in the groupBy, else use the best option
    RelBuilder relB = makeRelBuilder();
    relB.push(input.relNode);
    int timestampIdx = input.timestamp.getBestCandidate(relB);
    for (int cand : input.timestamp.getCandidates()) {
      if (groupByIdx.contains(cand)) {
        timestampIdx = cand;
        break;
      }
    }

    Pair<PkAndSelect, Timestamps> addedTimestamp =
        addTimestampAggregate(relB, groupByIdx, timestampIdx, aggregateCalls);
    PkAndSelect pkSelect = addedTimestamp.getKey();
    Timestamps timestamp = addedTimestamp.getValue();

    TumbleAggregationHint.instantOf(timestampIdx).addTo(relB);

    NowFilter nowFilter = input.nowFilter.remap(IndexMap.singleton(timestampIdx,
        timestamp.getOnlyCandidate()));

    return AnnotatedLP.build(relB.build(), TableType.STREAM, pkSelect.pk, timestamp, pkSelect.select,
                input)
            .rootTable(input.rootTable)
            .nowFilter(nowFilter).build();
  }

  private Pair<Integer, Integer> findTimestampInGroupBy(
      List<Integer> groupByIdx, Timestamps timestamp, RelNode input) {
    //Determine if one of the groupBy keys is a timestamp
    Integer timestampIdx = null;
    int keyIdx = -1;
    for (int i = 0; i < groupByIdx.size(); i++) {
      int idx = groupByIdx.get(i);
      if (timestamp.isCandidate(idx)) {
        if (timestampIdx!=null) {
          errors.fatal(ErrorCode.NOT_YET_IMPLEMENTED, "Do not currently support grouping by "
              + "multiple timestamp columns: [%s] and [%s]",
              rexUtil.getFieldName(idx,input), rexUtil.getFieldName(timestampIdx,input));
        }
        timestampIdx = idx;
        keyIdx = i;
      }
    }
    if (timestampIdx == null) return null;
    return Pair.of(timestampIdx, keyIdx);
  }

  private AnnotatedLP handleTimeWindowAggregation(AnnotatedLP input,
      List<Integer> groupByIdx, List<AggregateCall> aggregateCalls, int targetLength,
                                                  int timestampIdx, int keyIdx) {
    LogicalProject inputProject = (LogicalProject) input.getRelNode();
    RexNode timeAgg = inputProject.getProjects().get(timestampIdx);
    TimeTumbleFunctionCall bucketFct = TimeTumbleFunctionCall.from(timeAgg,
        rexUtil.getBuilder()).orElseThrow(
        () -> errors.exception("Not a valid time aggregation function: %s", timeAgg)
    );

    //Fix timestamp (if not already fixed)
    Timestamps newTimestamp = Timestamps.ofFixed(keyIdx);
    //Now filters must be on the timestamp - this is an internal check
    Preconditions.checkArgument(input.nowFilter.isEmpty()
        || input.nowFilter.getTimestampIndex() == timestampIdx);
    NowFilter nowFilter = input.nowFilter.remap(
        IndexMap.singleton(timestampIdx, keyIdx));

    RelBuilder relB = makeRelBuilder();
    relB.push(input.relNode);
    relB.aggregate(relB.groupKey(Ints.toArray(groupByIdx)), aggregateCalls);
    SqrlTimeTumbleFunction.Specification windowSpec = bucketFct.getSpecification();
    TumbleAggregationHint.functionOf(timestampIdx,
        bucketFct.getTimestampColumnIndex(),
        windowSpec.getWindowWidthMillis(), windowSpec.getWindowOffsetMillis()).addTo(relB);
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
      Optional<TimestampAnalysis.MaxTimestamp> maxTimestamp, int targetLength) {

    RelBuilder relB = makeRelBuilder();
    relB.push(input.relNode);
    //Fix best timestamp (if not already fixed)
    Timestamps inputTimestamp = input.timestamp;
    int timestampIdx = maxTimestamp.map(MaxTimestamp::getTimestampIdx)
        .orElse(inputTimestamp.getBestCandidate(relB));

    if (!input.nowFilter.isEmpty() && exec.supports(EngineCapability.STREAM_WINDOW_AGGREGATION)) {
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

      TopNConstraint dedup = TopNConstraint.makeDeduplication(pkAndSelect.pk.asList(),
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
      Optional<TimestampAnalysis.MaxTimestamp> maxTimestamp, int targetLength) {
    //Standard aggregation produces a state table
    input = input.inlineNowFilter(makeRelBuilder(), exec);
    RelBuilder relB = makeRelBuilder();
    relB.push(input.relNode);
    Timestamps resultTimestamp;
    int selectLength = targetLength;
    if (maxTimestamp.isPresent()) {
      TimestampAnalysis.MaxTimestamp mt = maxTimestamp.get();
      resultTimestamp = Timestamps.ofFixed(groupByIdx.size() + mt.getAggCallIdx());
    } else { //add max timestamp
      int bestTimestampIdx = input.timestamp.getBestCandidate(relB);
      //New timestamp column is added at the end
      resultTimestamp = Timestamps.ofFixed(targetLength);
      AggregateCall maxTimestampAgg = rexUtil.makeMaxAggCall(bestTimestampIdx,
          ReservedName.SYSTEM_TIMESTAMP.getCanonical(), groupByIdx.size(), relB.peek());
      aggregateCalls.add(maxTimestampAgg);
      targetLength++;
    }

    relB.aggregate(relB.groupKey(Ints.toArray(groupByIdx)), aggregateCalls);
    PkAndSelect pkSelect = aggregatePkAndSelect(groupByIdx, selectLength);
    return AnnotatedLP.build(relB.build(), TableType.STATE, pkSelect.pk,
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
