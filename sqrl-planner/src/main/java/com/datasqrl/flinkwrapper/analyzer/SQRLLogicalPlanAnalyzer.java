/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.flinkwrapper.analyzer;

import static com.datasqrl.io.tables.TableType.STATE;
import static com.datasqrl.io.tables.TableType.STREAM;
import static com.datasqrl.util.CalciteUtil.CAST_TRANSFORM;
import static com.datasqrl.util.CalciteUtil.COALESCE_TRANSFORM;

import com.datasqrl.flinkwrapper.TableAnalysisLookup;
import com.datasqrl.plan.rules.SqrlRelShuttle;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import com.datasqrl.calcite.SqrlRexUtil;
import com.datasqrl.calcite.SqrlRexUtil.JoinConditionDecomposition;
import com.datasqrl.calcite.SqrlRexUtil.JoinConditionDecomposition.EqualityCondition;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.flinkwrapper.hint.PlannerHints;
import com.datasqrl.flinkwrapper.hint.PrimaryKeyHint;
import com.datasqrl.flinkwrapper.parser.StatementParserException;
import com.datasqrl.flinkwrapper.tables.SqrlTableFunction;
import com.datasqrl.io.tables.TableType;
import com.datasqrl.plan.hints.JoinCostHint;
import com.datasqrl.plan.rules.AnnotatedLP;
import com.datasqrl.plan.rules.JoinAnalysis.Side;
import com.datasqrl.plan.table.PullupOperator;
import com.datasqrl.plan.util.IndexMap;
import com.datasqrl.plan.util.PrimaryKeyMap;
import com.datasqrl.plan.util.PrimaryKeyMap.ColumnSet;
import com.datasqrl.util.CalciteUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedHashMultimap;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.NonNull;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSnapshot;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.planner.functions.sql.SqlWindowTableFunction;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;

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
@Slf4j
public class SQRLLogicalPlanAnalyzer implements SqrlRelShuttle {

  final RelNode relNode;
  final SqrlRexUtil rexUtil;
  final TableAnalysisLookup tableLookup;
  final ErrorCollector errors;

  private final List<TableAnalysis> sourceTables = new ArrayList<>();
  private final CapabilityAnalysis capabilityAnalysis = new CapabilityAnalysis();
  private final List<JoinCostHint> costHints = new ArrayList<>(); //TODO: Generalize to general cost hints

  protected RelNodeAnalysis intermediateAnalysis = null;


  public SQRLLogicalPlanAnalyzer(@NonNull RelNode relNode, @NonNull TableAnalysisLookup tableLookup,
      @NonNull ErrorCollector errors) {
    this.relNode = relNode;
    this.rexUtil = new SqrlRexUtil(relNode.getCluster().getRexBuilder());
    this.tableLookup = tableLookup;
    this.errors = errors;
  }

  public TableAnalysis analyze(ObjectIdentifier identifier, PlannerHints hints) {
    RelNodeAnalysis analysis = analyzeRelNode(relNode);

    if (analysis.type.isStream() && CalciteUtil.findBestRowTimeIndex(analysis.relNode.getRowType()).isEmpty()) {
      //If we don't have a rowtime, let's check if we lost it when all inputs had a rowtime
      if (sourceTables.stream().filter(TableAnalysis::hasRowType).map(TableAnalysis::getRowType)
          .map(CalciteUtil::findBestRowTimeIndex).allMatch(Optional::isPresent)) {
        errors.notice("This table does not propagate the source row time columns: %s",
            sourceTables.stream().map(tbl -> tbl.getRowTime().map(tbl::getFieldName)).collect(
                Collectors.toList()));
      }
    }

    //See if the primary key is being explicitly set:
    Optional<PrimaryKeyHint> pkHint = hints.getHint(PrimaryKeyHint.class);
    if (pkHint.isPresent()) {
      try {
        analysis = analysis.toBuilder().primaryKey(
            PrimaryKeyMap.of(pkHint.get().getPkColumns(),analysis.getRelNode().getRowType())).build();
      } catch (IllegalArgumentException e) {
        throw new StatementParserException(pkHint.get().getSource().getFileLocation(), e);
      }
    }
    return new TableAnalysis(identifier,
        analysis.relNode, analysis.type, analysis.primaryKey,
        analysis.isMostRecentDistinct, analysis.streamRoot,
        sourceTables, Optional.empty(),
        capabilityAnalysis.getRequiredCapabilities(), hints);
  }

  /**
   * Because Flink automatically expands views we are trying to "undo" this here
   * by detecting whether a particular RelNode tree is associated with a previously defined table
   * through the tableLookup interface.
   *
   * @param input
   * @return
   */
  protected RelNodeAnalysis analyzeRelNode(RelNode input) {
    Optional<TableAnalysis> tableAnalysis = tableLookup.lookupTable(input);
    if (tableAnalysis.isPresent()) {
      sourceTables.add(tableAnalysis.get());
      return tableAnalysis.get().toRelNode(input);
    } else {
      input.accept(this);
      RelNodeAnalysis analysis = this.intermediateAnalysis;
      this.intermediateAnalysis = null;
      return analysis;
    }
  }

  protected RelNodeAnalysis getInputAnalysis(SingleRel relNode) {
    return analyzeRelNode(relNode.getInput());
  }

  protected List<RelNodeAnalysis> getInputAnalyses(RelNode relNode) {
    return relNode.getInputs().stream().map(this::analyzeRelNode).collect(Collectors.toList());
  }

  protected RelNode setProcessResult(RelNodeAnalysis analysis) {
    this.intermediateAnalysis = analysis;
    RelNode result = analysis.relNode;
    //Some sanity checks
    errors.checkFatal(analysis.type!=TableType.LOOKUP || result instanceof LogicalTableScan, "Lookup tables can only be used in temporal joins");
    return result;
  }


  @Override
  public RelNode visit(RelNode relNode) {
    if (relNode instanceof TableFunctionScan) {
      return visit((TableFunctionScan) relNode);
    } else if (relNode instanceof Uncollect) {
      return setProcessResult(RelNodeAnalysis.builder().type(TableType.STATIC).relNode(relNode).build());
    }

    //Default handling: if it has a single child, pass through, else use defaults
    if (relNode.getInputs().size()==1) {
      RelNodeAnalysis child = getInputAnalyses(relNode).get(0);
      return setProcessResult(child.toBuilder().relNode(relNode).isMostRecentDistinct(false).build());
    } else {
      return setProcessResult(RelNodeAnalysis.builder().relNode(relNode).build());
    }
  }

  @Override
  public RelNode visit(TableScan tableScan) {
    RelOptTable table = tableScan.getTable();
    Preconditions.checkArgument(table != null && table instanceof TableSourceTable);
    ObjectIdentifier tablePath = ((TableSourceTable)table).contextResolvedTable().getIdentifier();
    TableAnalysis tableAnalysis = tableLookup.lookupSourceTable(tablePath);
    errors.checkFatal(tableAnalysis!=null, "Could not find table: %s", tablePath);
    sourceTables.add(tableAnalysis);
    return sourceTable(tableAnalysis.toRelNode(tableScan));
  }

  private RelNode sourceTable(RelNodeAnalysis tableAnalysis) {
    if (CalciteUtil.hasNestedTable(tableAnalysis.getRelNode().getRowType())) {
      capabilityAnalysis.add(EngineFeature.DENORMALIZE);
    }
    return setProcessResult(tableAnalysis);
  }


  @Override
  public RelNode visit(TableFunctionScan functionScan) {
    RexCall call = (RexCall) functionScan.getCall();
    if (call.getOperator() instanceof SqlUserDefinedTableFunction) {
      TableFunction tableFunction = ((SqlUserDefinedTableFunction)call.getOperator()).getFunction();
      if (tableFunction instanceof SqrlTableFunction) {
        capabilityAnalysis.add(EngineFeature.TABLE_FUNCTION_SCAN); //We only support table functions on the read side
        SqrlTableFunction sqrlFct = (SqrlTableFunction) tableFunction;
        return sourceTable(sqrlFct.getTableAnalysis().toRelNode(functionScan));
      }
    } else if (call.getOperator() instanceof SqlWindowTableFunction) {
      //It's a flink time window function
      SqlWindowTableFunction windowFunction = (SqlWindowTableFunction) call.getOperator();
      capabilityAnalysis.add(EngineFeature.STREAM_WINDOW_AGGREGATION);
      if (functionScan.getInputs().size()==1) {
        RelNodeAnalysis input = getInputAnalyses(functionScan).get(0);
        if (input.hasNowFilter) errors.notice("Rewrite now-filter followed by a window aggregation to a sliding time window");
        return setProcessResult(input.toBuilder().relNode(functionScan).isMostRecentDistinct(false).hasNowFilter(false).build());
      }
    }
    //Generic table function call
    return setProcessResult(RelNodeAnalysis.builder().relNode(functionScan).build());
  }

  @Override
  public RelNode visit(LogicalValues logicalValues) {
    PrimaryKeyMap pk = determinePK(logicalValues);
    return setProcessResult(RelNodeAnalysis.builder().relNode(logicalValues).type(TableType.STATIC)
        .primaryKey(pk).build());
  }

  /**
   * Determines the primary key of values using the simple heuristic of selecting
   * the first non-null scalar column.
   *
   * @param logicalValues
   * @return
   */

  private PrimaryKeyMap determinePK(LogicalValues logicalValues) {
    ImmutableList<ImmutableList<RexLiteral>> tuples = logicalValues.getTuples();
    if (tuples.size()<=1) return PrimaryKeyMap.none();
    RelDataType rowType = logicalValues.getRowType();
    List<RelDataTypeField> fields = rowType.getFieldList();
    for (int i = 0; i < fields.size(); i++) {
      RelDataType type = fields.get(i).getType();
      if (!CalciteUtil.isPotentialPrimaryKeyType(type)) continue;
      //TODO: add check for unique column values
      return PrimaryKeyMap.of(List.of(i));
    }
    return PrimaryKeyMap.UNDEFINED;
  }

  private static final SqrlRexUtil.RexFinder FIND_NOW = SqrlRexUtil.findFunction(SqrlRexUtil::isNOW);
  private static final SqrlRexUtil.RexFinder FIND_ROWTIME_REF = SqrlRexUtil.findInputRef(ref -> CalciteUtil.isRowTime(ref.getType()));

  private final boolean isTableScan(RelNode relNode) {
    if (relNode instanceof TableScan) {
      return true;
    } else if (relNode instanceof LogicalWatermarkAssigner) {
      return isTableScan(((LogicalWatermarkAssigner) relNode).getInput());
    } else if (relNode instanceof LogicalProject) {
      //check if it is trivial
      LogicalProject project = (LogicalProject) relNode;
      if (!project.getRowType().equals(project.getInput().getRowType())) return false;
      for (int i = 0; i < project.getProjects().size(); i++) {
        if (CalciteUtil.getInputRef(project.getProjects().get(i)).orElse(-1)!=i) return false;
      }
      return true;
    }
    return false;
  }

  @Override
  public RelNode visit(LogicalFilter logicalFilter) {
    RelNodeAnalysis input = getInputAnalysis(logicalFilter);

    RexNode condition = logicalFilter.getCondition();
    List<RexNode> conjunctions = rexUtil.getConjunctions(condition);
    capabilityAnalysis.analyzeRexNode(conjunctions);

    //Identify any columns that are constrained to a constant value and a) remove as pk if they are or b) update pk if filter is on row_number
    Set<PrimaryKeyMap.ColumnSet> pksToRemove = new HashSet<>();
    List<Integer> newPk = null;
    boolean isMostRecentDistinct = false;
    for (RexNode node : conjunctions) {
      Optional<Integer> idxOpt = CalciteUtil.isEqualToConstant(node);
      //It's a constrained primary key, remove it from the list
      idxOpt.flatMap(input.primaryKey::getForIndex).ifPresent(pksToRemove::add);
      //Check if this is the row_number of a partitioned window-over
      if (idxOpt.isPresent() && input.relNode instanceof LogicalProject) {
        LogicalProject project = (LogicalProject) input.relNode;
        RexNode column = project.getProjects().get(idxOpt.orElseThrow());
        //to be most recent distinct: a) only single filter on row_number, b) all other projects are RexInputRef,
        // c) over is ordered by timestamp DESC and d) the input must be a table scan.
        isMostRecentDistinct = conjunctions.size()==1 //a
            && IntStream.range(0, project.getProjects().size()).filter(i -> i!= idxOpt.get())
            .mapToObj(project.getProjects()::get).map(CalciteUtil::getInputRef).allMatch(Optional::isPresent) //b
            && isTableScan(project.getInput()); //d
        if (column instanceof RexOver && column.isA(SqlKind.ROW_NUMBER)) {
          RexWindow window = ((RexOver) column).getWindow();
          newPk = window.partitionKeys.stream().map(n ->
                  CalciteUtil.getInputRefThroughTransform(n, List.of(CAST_TRANSFORM, COALESCE_TRANSFORM)))
              .map(opt -> opt.orElse(-1)).collect(Collectors.toUnmodifiableList());
          if (newPk.stream().anyMatch(idx -> idx<0)) {
            newPk = null; // Not all partition RexNodes are input refs
            isMostRecentDistinct = false;
          }
          if (window.orderKeys.isEmpty()) isMostRecentDistinct = false;
          else {
            RexFieldCollation collation = window.orderKeys.get(0);
            if (!collation.getDirection().isDescending()
                || !CalciteUtil.isRowTime(collation.getKey().getType())) {
              isMostRecentDistinct = false;
            }
          }
        }
      }

    }
    PrimaryKeyMap pk = input.primaryKey;
    TableType type = input.getType();
    if (newPk != null) {
      pk = PrimaryKeyMap.of(newPk);
      if (type== STREAM) { //Update type
        type = TableType.VERSIONED_STATE;
      }
    } else if (!pksToRemove.isEmpty()) { //Remove them
      pk = new PrimaryKeyMap(pk.asList().stream().filter(Predicate.not(pksToRemove::contains)).collect(
          Collectors.toList()));
    }

    boolean hasNowFilter = FIND_NOW.foundIn(condition);
    return setProcessResult(input.toBuilder().relNode(logicalFilter).type(type).primaryKey(pk).isMostRecentDistinct(isMostRecentDistinct)
        .hasNowFilter(hasNowFilter).build());
  }

  @Override
  public RelNode visit(LogicalCalc logicalCalc) {
    log.warn("Logical Calc not expected during initial planning");
    return visit((RelNode) logicalCalc); //Default treatment
  }

  @Override
  public RelNode visit(LogicalProject logicalProject) {
    RelNodeAnalysis input = getInputAnalysis(logicalProject);
    RelDataType inputType = input.relNode.getRowType();
    RelDataType resultType = logicalProject.getRowType();
    //Keep track of mappings
    LinkedHashMultimap<Integer, Integer> mappedProjects = LinkedHashMultimap.create();

    boolean isTrivialProject = true;
    for (Ord<RexNode> exp : Ord.<RexNode>zip(logicalProject.getProjects())) {
      CalciteUtil.getNonAlteredInputRef(exp.e)
          .ifPresent(originalIndex -> mappedProjects.put(originalIndex, exp.i));
      //if this isn't mapping to the same underlying column, it's not trivial
      if (CalciteUtil.getInputRef(exp.e).filter(
          idx -> resultType.getFieldNames().get(exp.i)
              .equalsIgnoreCase(inputType.getFieldNames().get(idx))).isEmpty()) isTrivialProject = false;
    }
    if (isTrivialProject && inputType.equals(resultType)) {
      return setProcessResult(input);
    }
    //Map the primary key columns
    PrimaryKeyMap pk = PrimaryKeyMap.UNDEFINED;
    boolean lostPrimaryKeyMapping = false;
    if (input.primaryKey.isDefined()) {
      PrimaryKeyMap.Builder pkBuilder = PrimaryKeyMap.build();
      for (PrimaryKeyMap.ColumnSet colSet : input.primaryKey.asList()) {
        Set<Integer> mappedTo = colSet.getIndexes().stream()
            .flatMap(idx -> mappedProjects.get(idx).stream())
            .collect(Collectors.toUnmodifiableSet());
        if (mappedTo.isEmpty()) {
          lostPrimaryKeyMapping = true;
          int pkIdx = colSet.pickBest(inputType);
          errors.notice("Primary key column [%s] is not selected",
              inputType.getFieldList().get(pkIdx).getName());
        } else {
          pkBuilder.add(mappedTo);
        }
      }
      if (!lostPrimaryKeyMapping) pk = pkBuilder.build();
    }
    RelNodeAnalysis.RelNodeAnalysisBuilder builder = input.toBuilder().relNode(logicalProject).primaryKey(pk);
    if (!isTrivialProject || lostPrimaryKeyMapping) builder.isMostRecentDistinct(false);
    return setProcessResult(builder.build());
  }

  @Override
  public RelNode visit(LogicalJoin logicalJoin) {

    RelNodeAnalysis leftIn = analyzeRelNode(logicalJoin.getLeft());
    RelNodeAnalysis rightIn = analyzeRelNode(logicalJoin.getRight());
    JoinRelType joinType = logicalJoin.getJoinType();

    Optional<TableAnalysis> rootTable = Optional.empty();

    final int leftSideMaxIdx = leftIn.getFieldLength();
    final IndexMap remapRightSide = idx -> idx + leftSideMaxIdx;
    RexNode condition = logicalJoin.getCondition();
    capabilityAnalysis.analyzeRexNode(condition);
    JoinConditionDecomposition eqDecomp = rexUtil.decomposeJoinCondition(
        condition, leftSideMaxIdx);

    /*We are going to detect if all the pk columns on the left or right hand side of the join
      are covered by equality constraints since that determines the resulting pk and is used
      in temporal join detection */
    EnumMap<Side,Boolean> isPKConstrained = new EnumMap<>(Side.class);
    for (Side side : new Side[]{Side.LEFT, Side.RIGHT}) {
      RelNodeAnalysis constrainedInput;
      Function<EqualityCondition,Integer> getEqualityIdx;
      IndexMap remapPk;
      if (side == Side.LEFT) {
        constrainedInput = leftIn;
        remapPk = IndexMap.IDENTITY;
        getEqualityIdx = EqualityCondition::getLeftIndex;
      } else {
        assert side==Side.RIGHT;
        constrainedInput = rightIn;
        remapPk = remapRightSide;
        getEqualityIdx = EqualityCondition::getRightIndex;
      }
      Set<Integer> pkEqualities = eqDecomp.getEqualities().stream().map(getEqualityIdx)
          .collect(Collectors.toSet());
      boolean allCovered = constrainedInput.primaryKey.isDefined() && constrainedInput.primaryKey.asList().stream()
              .map(col -> col.remap(remapPk)).allMatch(col -> col.containsAny(pkEqualities));
      isPKConstrained.put(side,allCovered);
    }

    //Determine the joined primary key by removing pk columns that are constrained by the join condition
    PrimaryKeyMap joinedPk = PrimaryKeyMap.UNDEFINED;
    if (leftIn.primaryKey.isDefined() && rightIn.primaryKey.isDefined()) {
      Set<Integer> joinedPKIdx = new HashSet<>();
      List<PrimaryKeyMap.ColumnSet> combinedPkColumns = new ArrayList<>();
      combinedPkColumns.addAll(leftIn.primaryKey.asList());
      combinedPkColumns.addAll(rightIn.primaryKey.remap(remapRightSide).asList());
      Set<Integer> constrainedColumns = eqDecomp.getEqualities().stream().map(eq -> eq.isTwoSided()?eq.getRightIndex():eq.getOneSidedIndex())
          .collect(Collectors.toUnmodifiableSet());
      combinedPkColumns.removeIf(columnSet -> columnSet.containsAny(constrainedColumns));
      joinedPk = new PrimaryKeyMap(combinedPkColumns);
    }

    //What is this for??
    Side singletonSide = Side.NONE;
    for (Side side : new Side[]{Side.LEFT, Side.RIGHT}) {
      if (isPKConstrained.get(side)) singletonSide = side;
    }



    //See if this join could be written as a temporal join and detect temporal join
    boolean isTemporalJoin = false;
    if ((leftIn.type.isStream() && rightIn.type.supportsTemporalJoin() && isPKConstrained.get(Side.RIGHT))
       || (rightIn.type.isStream() && leftIn.type.supportsTemporalJoin() && isPKConstrained.get(Side.LEFT))) {
      //This could be a temporal join, check for snapshot
      RelNodeAnalysis temporalSide = leftIn.type.isStream()?rightIn:leftIn;
      RelNodeAnalysis streamSide = leftIn.type.isStream()?leftIn:rightIn;
      if (temporalSide.getRelNode() instanceof LogicalSnapshot) {
        //TODO: do we need to check that the right rowtime was chosen?
        isTemporalJoin = true;
        rootTable = streamSide.streamRoot;
      } else {
        RelDataType streamType = streamSide.getRelNode().getRowType();
        Optional<Integer> rowTimeIdx = CalciteUtil.findBestRowTimeIndex(streamType);
        rowTimeIdx.ifPresent(integer -> errors.notice(
            "You can rewrite the join as a temporal join for greater efficiency by adding: FOR SYSTEM_TIME AS OF `%s`",
            streamType.getFieldList().get(integer).getName()));
      }
    }

    //Detect interval join
    if (leftIn.type.isStream() && rightIn.type.isStream()) {
      //Check if the join condition contains time bounds - we use an approximation and see if
      //the condition references any rowtime columns
      boolean hasRowTimeConstraint = FIND_ROWTIME_REF.foundIn(condition);
      boolean sharesRootWithPkConstraint = false;
      /*
      Detect a special case where we are joining two child tables of the same root table (i.e. we
      have equality constraints on the root pk columns for both sides). In that case, we are guaranteed
      that the timestamps must be identical and we can add that condition to convert the join to
      an interval join.
       */
      if (identicalStreamRoots(leftIn.streamRoot, rightIn.streamRoot)) {
        int numRootPks = leftIn.streamRoot.get().getPrimaryKey().getLength();
        if (leftIn.primaryKey.isDefined() && rightIn.primaryKey.isDefined()
            && leftIn.primaryKey.getLength()>=numRootPks
            && rightIn.primaryKey.getLength()>=numRootPks) {
          List<PrimaryKeyMap.ColumnSet> leftRootPks = leftIn.primaryKey.asSubList(numRootPks);
          List<PrimaryKeyMap.ColumnSet> rightRootPks = rightIn.primaryKey.asSubList(numRootPks)
              .stream().map(col -> col.remap(idx -> idx + leftSideMaxIdx))
              .collect(Collectors.toUnmodifiableList());
          sharesRootWithPkConstraint = true;
          for (int i = 0; i < numRootPks; i++) {
            PrimaryKeyMap.ColumnSet left = leftRootPks.get(i), right = rightRootPks.get(i);
            if (eqDecomp.getEqualities().stream().noneMatch(
                eq -> left.contains(eq.getLeftIndex()) && right.contains(eq.getRightIndex()))) {
              sharesRootWithPkConstraint = false;
              break;
            }
          }
          if (sharesRootWithPkConstraint) {
            rootTable = leftIn.streamRoot;
          }
        }
      }

      if (!hasRowTimeConstraint) {
        if (sharesRootWithPkConstraint) {
          Function<RelDataType, Optional<String>> findRowTimeCol =
              dt -> CalciteUtil.findBestRowTimeIndex(dt).map(dt.getFieldList()::get)
                  .map(RelDataTypeField::getName);
          Optional<String> leftName = findRowTimeCol.apply(leftIn.relNode.getRowType()),
              rightName = findRowTimeCol.apply(rightIn.relNode.getRowType());
          if (leftName.isPresent() && rightName.isPresent())
            errors.notice("Add `%s = %s` JOIN condition to significantly improve performance",
                leftName.get(), rightName.get());
        } else {
          //TODO: add notice for inefficiency?
        }

      }
    }


    TableType resultType;
    if (isTemporalJoin) {
      resultType = STREAM;
    } else if (rightIn.type.isStream() && leftIn.type.isStream()) {
      resultType = joinType.isOuterJoin()?TableType.STATE: STREAM;
    } else {
      resultType = rightIn.type.combine(leftIn.type);
    }

    if (rightIn.type==TableType.STATIC) {
      rootTable = leftIn.streamRoot;
    } else if (leftIn.type==TableType.STATIC) {
      rootTable = rightIn.streamRoot;
    }

    //Default joins without primary key constraints or interval bounds can be expensive, so we create a hint for the cost model
    costHints.add(new JoinCostHint(leftIn.type, rightIn.type, eqDecomp.getEqualities().size(), singletonSide));

    return setProcessResult(new RelNodeAnalysis(logicalJoin, resultType, joinedPk,
        false, rootTable, leftIn.hasNowFilter || rightIn.hasNowFilter
        ));
  }

  private boolean identicalStreamRoots(Optional<TableAnalysis> leftRoot, Optional<TableAnalysis> rightRoot) {
    return leftRoot.filter(
        left -> rightRoot.filter(right -> right.equals(left)).isPresent()).isPresent();
  }

  @Override
  public RelNode visit(LogicalCorrelate logicalCorrelate) {
    RelNodeAnalysis leftIn = analyzeRelNode(logicalCorrelate.getLeft());
    RelNodeAnalysis rightIn = analyzeRelNode(logicalCorrelate.getRight());

    final int leftSideMaxIdx = leftIn.getFieldLength();
    PrimaryKeyMap pk = PrimaryKeyMap.UNDEFINED;
    if (leftIn.primaryKey.isDefined() && rightIn.primaryKey.isDefined()) {
      PrimaryKeyMap.Builder pkBuilder = leftIn.getPrimaryKey().toBuilder();
      pkBuilder.addAll(rightIn.getPrimaryKey().remap(idx -> idx + leftSideMaxIdx).asList());
      pk = pkBuilder.build();
    }

    return setProcessResult(new RelNodeAnalysis(logicalCorrelate, leftIn.type, pk,
        false, leftIn.getStreamRoot(), leftIn.hasNowFilter || rightIn.hasNowFilter
    ));
  }

  private PrimaryKeyMap intersectPrimaryKeys(List<RelNodeAnalysis> inputs) {
    if (inputs.get(0).primaryKey.isUndefined()) return PrimaryKeyMap.UNDEFINED;
    int pkLength = inputs.get(0).primaryKey.getLength();
    if (inputs.stream().allMatch(in -> in.primaryKey.getLength()==pkLength)) {
      PrimaryKeyMap.Builder pkBuilder = PrimaryKeyMap.build();
      //Compute the intersection of all column sets
      for (int i = 0; i < pkLength; i++) {
        ColumnSet colset = inputs.get(0).primaryKey.get(i);
        for (int j = 1; j < inputs.size(); j++) {
          colset = colset.intersect(inputs.get(j).primaryKey.get(i));
          if (colset.isEmpty()) return PrimaryKeyMap.UNDEFINED;
        }
        pkBuilder.add(colset);
      }
      return pkBuilder.build();
    }
    return PrimaryKeyMap.UNDEFINED;
  }

  public RelNode visitSetOperation(RelNode setOperation, boolean preservesStream) {
    List<RelNodeAnalysis> inputs = getInputAnalyses(setOperation);
    TableType resultType = TableType.STATE;
    if (inputs.stream().allMatch(in -> in.type == TableType.STATIC)) {
      resultType = TableType.STATIC;
    } else if (inputs.stream().anyMatch(in -> in.type == TableType.RELATION)) {
      resultType = TableType.RELATION;
    } else if (preservesStream && inputs.stream().allMatch(in -> in.type == STREAM)) {
      resultType = STREAM;
    }
    //Check if we have an intersection of primary keys
    PrimaryKeyMap pk = intersectPrimaryKeys(inputs);

    boolean nowFilter = inputs.stream().anyMatch(in -> in.hasNowFilter);
    //Check if all stream roots are identical
    Optional<TableAnalysis> streamRoot = inputs.get(0).streamRoot;
    for (int i = 1; i < inputs.size(); i++) {
      if (!identicalStreamRoots(streamRoot, inputs.get(i).streamRoot)) {
        streamRoot = Optional.empty();
      }
    }
    return setProcessResult(new RelNodeAnalysis(setOperation, resultType, pk, false, streamRoot, nowFilter));
  }

  @Override
  public RelNode visit(LogicalUnion logicalUnion) {
    return visitSetOperation(logicalUnion, logicalUnion.all);
  }

  @Override
  public RelNode visit(LogicalIntersect logicalIntersect) {
    return visitSetOperation(logicalIntersect, false);
  }

  @Override
  public RelNode visit(LogicalMinus logicalMinus) {
    return visitSetOperation(logicalMinus, false);
  }

  @Override
  public RelNode visit(LogicalAggregate aggregate) {
    RelNodeAnalysis input = getInputAnalysis(aggregate);
    capabilityAnalysis.analyzeAggregates(aggregate.getAggCallList());
    final List<Integer> groupByIdx = aggregate.getGroupSet().asList();

    /*
      Produces the pk and select mappings by taking into consideration that the group-by indexes of
      an aggregation are implicitly sorted because they get converted to a bitset in the RelBuilder.
    */
     List<Integer> finalGroupByIdx = groupByIdx.stream().sorted().collect(Collectors.toList());
    Preconditions.checkArgument(groupByIdx.stream().map(finalGroupByIdx::indexOf).allMatch(idx ->idx >= 0),
        "Invalid groupByIdx [%s] to [%s]", groupByIdx, finalGroupByIdx);
    PrimaryKeyMap pk = PrimaryKeyMap.of(groupByIdx.stream().map(finalGroupByIdx::indexOf).collect(
        Collectors.toList()));

    Optional<TableAnalysis> streamRoot = Optional.empty();
    if (input.type == STREAM && input.streamRoot.isPresent()) {
      TableAnalysis rootTable = input.streamRoot.get();
      int numRootPks = rootTable.getPrimaryKey().getLength();
      //Check that all root primary keys are part of the groupBy
      if (IntStream.range(0, numRootPks).allMatch(i -> rootTable.getPrimaryKey().get(i).containsAny(groupByIdx))) {
        streamRoot = Optional.of(rootTable);
        //TODO: check if this is doing a time-window aggregation, otherwise suggest one with 1ms window
        //See SQRLLogicalPlanRewriter#handleNestedAggregationInStream for inspiration
      }
    }

    List<String> groupByFieldNames = groupByIdx.stream().map(input::getFieldName).map(String::toLowerCase).collect(Collectors.toList());
    TableType resultType = TableType.STATE;
    //make stream if aggregation is on TVF with window start, end
    if (input.type==STREAM && groupByFieldNames.contains("window_start") && groupByFieldNames.contains("window_end")) {
      resultType = STREAM;
    }
    return setProcessResult(new RelNodeAnalysis(aggregate, resultType, pk, false, streamRoot, false));
  }

  @Override
  public RelNode visit(LogicalMatch logicalMatch) {
    //Check if this is an event time
    RelNodeAnalysis input = getInputAnalysis(logicalMatch);
    if (input.type.isStream()) {
      Optional<RelFieldCollation> firstOrder = logicalMatch.getOrderKeys().getFieldCollations().stream().findFirst();
      if (firstOrder.isPresent() && CalciteUtil.isRowTime(input.getField(firstOrder.get().getFieldIndex()).getType())) {
        return setProcessResult(RelNodeAnalysis.builder().type(STREAM).relNode(logicalMatch).build());
      } else {
        errors.notice("For efficient pattern matching, add `ORDER BY %s`",
            CalciteUtil.findBestRowTimeIndex(input.relNode.getRowType()).map(input::getFieldName)
                .orElse("[propagate timestamp column]"));
      }
    } else {
      errors.notice("For efficient pattern matching, make sure the input data is a stream");
    }
    return setProcessResult(RelNodeAnalysis.builder().type(STATE).relNode(logicalMatch).build());
  }

  @Override
  public RelNode visit(LogicalSort logicalSort) {
    //TODO: add cost hint
    return visit((RelNode) logicalSort);
  }

  @Override
  public RelNode visit(LogicalExchange logicalExchange) {
    log.warn("LogicExchange not expected during initial processing");
    return visit((RelNode) logicalExchange);
  }

}
