/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.planner.dag;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.EngineType;
import com.datasqrl.config.PackageJson;
import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.datatype.DataTypeMapping.Direction;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlan.PhysicalStagePlan;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.export.ExportEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.flinkrunner.stdlib.commons.hash_columns;
import com.datasqrl.plan.global.StageAnalysis;
import com.datasqrl.plan.util.PrimaryKeyMap;
import com.datasqrl.planner.Sqrl2FlinkSQLTranslator;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.dag.nodes.ExportNode;
import com.datasqrl.planner.dag.nodes.PipelineNode;
import com.datasqrl.planner.dag.nodes.PlannedNode;
import com.datasqrl.planner.dag.nodes.TableFunctionNode;
import com.datasqrl.planner.dag.nodes.TableNode;
import com.datasqrl.planner.dag.plan.MaterializationStagePlan;
import com.datasqrl.planner.dag.plan.MaterializationStagePlan.MaterializationStagePlanBuilder;
import com.datasqrl.planner.dag.plan.MaterializationStagePlan.Query;
import com.datasqrl.planner.dag.plan.ServerStagePlan;
import com.datasqrl.planner.hint.PartitionKeyHint;
import com.datasqrl.planner.parser.AccessModifier;
import com.datasqrl.planner.tables.AccessVisibility;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import com.datasqrl.planner.tables.SqrlTableFunction;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.FlinkCompileException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.schema.TimeIndicatorRelDataType;
import org.springframework.stereotype.Component;

/** Optimizes the DAG and produces the physical plan after DAG cutting */
@Component
@AllArgsConstructor(onConstructor_ = @Inject)
public class DAGPlanner {

  public static final String UNIQUE_TABLE_APPENDIX = "_"; // TODO: add $ to ensure uniqueness?

  private static final FunctionDefinition HASH_COLUMNS = new hash_columns();

  private final ExecutionPipeline pipeline;
  private final ErrorCollector errors;
  private final PackageJson packageJson;

  /**
   * Eliminates unreachable nodes from the DAG and determines all viable execution stages for each
   * node in the DAG. It then picks the most cost-efficient stage assignment using a greedy
   * heuristic.
   *
   * @param dag
   * @return
   */
  public PipelineDAG optimize(PipelineDAG dag) {
    dag = dag.trimToSinks();
    for (PipelineNode node : dag) {
      if (!node.hasViableStage()) {
        errors.fatal(
            "Could not find execution stage for [%s]. Stage analysis below.\n%s",
            node.getName(), node.toString());
      }
    }
    try {
      dag.eliminateInviableStages(pipeline);
    } catch (PipelineDAG.NoPlanException ex) {
      // Print error message that is easy to read
      errors.fatal(
          "Could not find execution stage for [%s]. Full DAG below.\n%s",
          ex.getNode().getName(), dag);
    }
    dag.forEach(node -> Preconditions.checkArgument(node.hasViableStage()));

    // Pick most cost-effective stage for each node and assign
    for (PipelineNode node : dag) {
      if (node.setCheapestStage()) {
        // If we eliminated stages, we make sure to eliminate all inviable stages
        dag.eliminateInviableStages(pipeline);
      }
      // Assign stage to table
      if (node instanceof TableNode tableNode) {
        var table = tableNode.getTableAnalysis();
        var stage = node.getChosenStage();
        Preconditions.checkNotNull(stage);
        // table.assignStage(stage); //this stage on the config below
      }
    }
    dag.allNodesByClass(PipelineNode.class)
        .forEach(
            node ->
                errors.checkFatal(
                    node.getStageAnalysis().values().stream()
                            .filter(StageAnalysis::isSupported)
                            .count()
                        == 1,
                    "Node has more than one supported stage - something"
                        + "went wrong during DAG optimization: %s",
                    node));

    return dag;
  }

  /**
   * Produces the physical plans for all stages in the DAG. During the optimization stage, we
   * assigned stages to each node in the dag. This effectively "cuts" the DAG by stage and this
   * method produces the physical plan for each stage so that the data flows from one stage to the
   * next across those "cuts".
   *
   * @param dag
   * @param sqrlEnv
   * @return
   */
  public PhysicalPlan assemble(PipelineDAG dag, Sqrl2FlinkSQLTranslator sqrlEnv) {
    var streamStage = pipeline.getStageByType(EngineType.PROCESS).orElseThrow();
    var serverEngine = pipeline.getServerEngine();
    var serverPlan = ServerStagePlan.builder();
    List<ExecutionStage> dataStoreStages =
        pipeline.stages().stream().filter(stage -> stage.engine().getType().isDataStore()).toList();

    var engineUtils = new MaterializationStagePlan.Utils(sqrlEnv.getRexUtil());
    Map<ExecutionStage, MaterializationStagePlanBuilder> exportPlans =
        pipeline.stages().stream()
            .filter(stage -> stage.engine().getType().supportsExport())
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    s -> MaterializationStagePlan.builder().utils(engineUtils).stage(s)));

    var mutationStage = pipeline.getMutationStage();
    if (mutationStage.isPresent()) {
      // Add mutations that were planned during DAG building
      var planBuilder = exportPlans.get(mutationStage.get());
      dag.allNodesByClass(TableNode.class)
          .flatMap(node -> node.getMutation().stream())
          .forEach(
              mut -> {
                planBuilder.mutation(mut.getCreateTopic());
                if (mut.isGenerateAccess()) {
                  serverPlan.mutation(mut);
                }
              });
    }

    Map<InputTableKey, ObjectIdentifier> streamTableMapping = new HashMap<>();
    final var exportTableCounter = new AtomicInteger(0);
    Function<String, String> uniqueNameFct =
        name -> {
          return name + UNIQUE_TABLE_APPENDIX + exportTableCounter.incrementAndGet();
        };
    var planBuilder = PhysicalPlan.builder();
    // move assembler logic here
    // ##1st: find all the cuts between flink and materialization (db+log) stages or sinks
    // generate a sink for each in the respective engine and insert into
    dag.allNodesByClassAndStage(TableNode.class, streamStage)
        .forEach(
            node -> {
              Set<ExecutionStage> downstreamStages =
                  new HashSet<>(); // We want to only plan once for each stage
              // We need stable iteration order for reproducibility
              List<PipelineNode> downstreamNodes = dag.getOutputs(node).stream().sorted().toList();
              for (PipelineNode downstream : downstreamNodes) {
                if (downstream instanceof ExportNode
                    || !downstream.getChosenStage().equals(streamStage)) {
                  // Create sink
                  ExecutionStage exportStage = null;
                  ObjectIdentifier targetTable = null;
                  String originalTableName = null;
                  if (downstream instanceof ExportNode exportNode) {
                    originalTableName = exportNode.getSinkPath().getLast().getDisplay();
                    if (exportNode.getSinkTo().isPresent()) {
                      exportStage = exportNode.getSinkTo().get();
                    } else {
                      // Special case, we sink directly to table
                      targetTable = exportNode.getCreatedSinkTable().get();
                      sqrlEnv.insertInto(
                          sqrlEnv.getTableScan(node.getIdentifier().objectIdentifier()).build(),
                          targetTable,
                          exportNode.getBatchIndex());
                      continue;
                    }
                  } else { // We are sinking into another engine
                    exportStage = downstream.getChosenStage();
                    originalTableName = node.getTableAnalysis().getName();
                    if (!downstreamStages.add(exportStage)) {
                      continue;
                    }
                  }
                  assert exportStage != null;
                  Preconditions.checkArgument(
                      exportStage.engine().getType().supportsExport(),
                      "Execution stage [%s] does not support exporting [%s]",
                      exportStage,
                      node);
                  var exportEngine = (ExportEngine) exportStage.engine();
                  TableAnalysis originalNodeTable = node.getTableAnalysis(),
                      sinkNodeTable = originalNodeTable;
                  if (exportEngine.supports(EngineFeature.MATERIALIZE_ON_KEY)
                      && node.getTableAnalysis().isMostRecentDistinct()) {
                    // If we are sinking to a datastore and the node is a most recent distinct, we
                    // can remove that node
                    // since materializing into the table on primary key has the same effect and is
                    // more efficient
                    errors.checkFatal(
                        node.getTableAnalysis().getPrimaryKey().isDefined(),
                        "Expected primary key: %s",
                        node);
                    sinkNodeTable =
                        ((TableNode) Iterables.getOnlyElement(dag.getInputs(node)))
                            .getTableAnalysis();
                  }
                  var relBuilder = sqrlEnv.getTableScan(sinkNodeTable.getObjectIdentifier());
                  var tblBuilder = new FlinkTableBuilder();
                  tblBuilder.setName(uniqueNameFct.apply(originalTableName));
                  // #1st: determine primary key and partition key (if present)
                  var pk = deterinePrimaryKey(originalNodeTable, relBuilder, sqrlEnv, exportStage);
                  if (pk.isDefined()) {
                    var fields = relBuilder.peek().getRowType().getFieldList();
                    List<String> pkColNames =
                        pk.asSimpleList().stream()
                            .map(fields::get)
                            .map(RelDataTypeField::getName)
                            .collect(Collectors.toList());
                    tblBuilder.setPrimaryKey(pkColNames);
                  }
                  if (exportEngine.supports(EngineFeature.PARTITIONING)) {
                    originalNodeTable
                        .getHints()
                        .getHint(PartitionKeyHint.class)
                        .ifPresent(
                            partitionKeyHint -> {
                              var partitionKey = partitionKeyHint.getColumnNames();
                              tblBuilder.setPartition(partitionKey);
                            });
                  }

                  // #2nd: apply type casting
                  mapTypes(
                      relBuilder,
                      sqrlEnv,
                      originalNodeTable.getRowTime().orElse(-1),
                      exportEngine.getTypeMapping(),
                      Direction.TO_DATABASE);

                  // #3rd: create table
                  var datatype = relBuilder.peek().getRowType();
                  tblBuilder.setColumns(datatype);
                  var createdTable =
                      exportEngine.createTable(
                          exportStage,
                          originalTableName,
                          tblBuilder,
                          datatype,
                          Optional.of(originalNodeTable));
                  exportPlans.get(exportStage).table(createdTable);
                  targetTable = sqrlEnv.createSinkTable(tblBuilder);
                  streamTableMapping.put(
                      new InputTableKey(exportStage, originalNodeTable.getObjectIdentifier()),
                      targetTable);
                  // Finally: add insert statement to sink into table
                  sqrlEnv.insertInto(relBuilder.build(), targetTable);
                }
              }
            });

    try {
      planBuilder.stagePlan(new PhysicalStagePlan(streamStage, sqrlEnv.compileFlinkPlan()));

    } catch (FlinkCompileException e) {
      e.setDag(dag.toString());
      throw e;
    }

    // 2nd: for each materialization stage, find all cuts to server or sinks generate queries for
    // those
    // This map keeps track of tables that are accessed by the server and hence mapped to
    // SqrlTableFunction
    Map<ObjectIdentifier, SqrlTableFunction> databaseTableMapping = new HashMap<>();
    for (ExecutionStage dataStoreStage : dataStoreStages) {
      var dbEngine = (DatabaseEngine) dataStoreStage.engine();
      var dbPlan = exportPlans.get(dataStoreStage);
      dag.allNodesByClassAndStage(PlannedNode.class, dataStoreStage)
          .forEach(
              node -> {
                SqrlTableFunction function = null;
                if (node instanceof TableFunctionNode functionNode) {
                  function = functionNode.getFunction();
                } else {
                  /*We also have to plan all TableNodes that have a downstream server consumer and convert
                   those to tablefunctions of the same name as the table, so they can be executed in the server
                  */
                  var tableNode = (TableNode) node;
                  var isQueriedByServer =
                      dag.getOutputs(tableNode).stream()
                          .anyMatch(
                              output -> output.getChosenStage().getType() == EngineType.SERVER);
                  if (isQueriedByServer) {
                    var tableid = tableNode.getIdentifier().objectIdentifier();
                    function =
                        SqrlTableFunction.builder()
                            .functionAnalysis(tableNode.getAnalysis())
                            .fullPath(NamePath.of(tableid.getObjectName()))
                            .visibility(
                                new AccessVisibility(AccessModifier.NONE, false, true, true))
                            .build();
                    databaseTableMapping.put(tableid, function);
                  }
                }
                if (function != null) {
                  var originalRelnode = function.getFunctionAnalysis().getRelNode();
                  RelNode plannedRelNode;
                  if (function.isPassthrough()) {
                    plannedRelNode = originalRelnode;
                  } else {
                    plannedRelNode =
                        originalRelnode.accept(
                            new QueryExpansionRelShuttle(
                                id -> streamTableMapping.get(new InputTableKey(dataStoreStage, id)),
                                sqrlEnv,
                                dbEngine.getTypeMapping(),
                                true));
                  }
                  dbPlan.query(
                      new Query(
                          function, plannedRelNode, function.getFunctionAnalysis().getErrors()));
                  if (function.getVisibility().isQueryable()) {
                    serverPlan.function(function);
                  }
                }
              });
      var dbPhysicalPlan = dbEngine.plan(dbPlan.build());
      planBuilder.stagePlan(new PhysicalStagePlan(dataStoreStage, dbPhysicalPlan));
    }

    // 3rd: build table functions for server stage
    if (serverEngine.isPresent()) {
      Preconditions.checkArgument(
          dag.allNodesByClass(PipelineNode.class)
              .noneMatch(stage -> stage.getChosenStage().getType() == EngineType.SERVER),
          "We do not yet support server side query execution");
      /*For now, we do not support execution on the server, hence we just pass the functions through
      since they must all be planned already. In the future, we could have SqrlTableFunctions that require
      server-side execution. Those may directly follow the stream stage, so we have to create a SqrlTableFunction as indicated by the comment above.
      Need to update SqlScriptPlanner availableStages for server to be included.
       */
      serverPlan.serverEngine(serverEngine.get());
      serverPlan.execFnFactory(sqrlEnv.getExecFnFactory());
      var serverPhysicalPlan = serverEngine.get().plan(serverPlan.build());
      planBuilder.stagePlan(
          new PhysicalStagePlan(
              pipeline.getStagesByType(EngineType.SERVER).stream().findFirst().get(),
              serverPhysicalPlan));
    }

    return planBuilder.build();
  }

  public static final String HASHED_PK_NAME = "__pk_hash";

  /**
   * Determines the primary key for a table that is written to a data system. The primary key is
   * important because it determines when two records are considered identical.
   *
   * <p>Some data systems, like databases, require a primary key for data to be accessed and
   * managed.
   *
   * @param table
   * @param relBuilder
   * @param sqrlEnv
   * @param stage
   * @return
   */
  private PrimaryKeyMap deterinePrimaryKey(
      TableAnalysis table,
      FlinkRelBuilder relBuilder,
      Sqrl2FlinkSQLTranslator sqrlEnv,
      ExecutionStage stage) {
    var pk = table.getSimplePrimaryKey();
    var numCols = table.getRowType().getFieldCount();
    List<Integer> addHashColumn = null;
    if (pk.isDefined() && pk.getLength() == 0) {
      // need to add a constant at the end so we have an actual pk column which is required by most
      // databases
      addHashColumn = List.of();
    } else if (pk.isUndefined()) {
      // Databases requires a primary key, see if we can create one
      if (stage.getType() == EngineType.DATABASE) {
        table
            .getErrors()
            .checkFatal(
                table.getType().isStream(),
                "Could not determine primary key for table [%s]. Please add a primary key with the /*+primary_key(...) */ hint.",
                table.getIdentifier().toString());
        // For stream tables, we hash all columns as primary key if none is explicitly defined or
        // inferred
        addHashColumn = IntStream.range(0, numCols).boxed().collect(Collectors.toList());
      } else {
        return pk; // We don't have and don't need a pk
      }
    } else {
      var hasNullPk =
          pk.asSimpleList().stream()
              .map(table::getField)
              .map(RelDataTypeField::getType)
              .anyMatch(RelDataType::isNullable);
      // we hash all the pk columns if it is required that they are not null
      if (hasNullPk && stage.supportsFeature(EngineFeature.REQUIRES_NOT_NULL_PRIMARY_KEY)) {
        addHashColumn = pk.asSimpleList();
      }
    }
    if (addHashColumn != null) {
      // add hash column
      if (!addHashColumn.isEmpty()) {
        CalciteUtil.addColumn(
            relBuilder,
            relBuilder
                .getRexBuilder()
                .makeCall(
                    sqrlEnv.lookupUserDefinedFunction(HASH_COLUMNS),
                    CalciteUtil.getSelectRex(relBuilder, addHashColumn)),
            HASHED_PK_NAME);
      } else {
        // add constant as pk
        CalciteUtil.addColumn(relBuilder, relBuilder.literal(1), HASHED_PK_NAME);
      }
      return PrimaryKeyMap.of(List.of(numCols));
    } else {
      return pk;
    }
  }

  /**
   * Maps the field types based on the provided data mapper so they can be written to the engine
   * sink.
   *
   * <p>TODO: Need to cast all but the first rowtime to avoid Flink exception
   *
   * @param relBuilder
   * @param sqrlEnv
   * @param timestampIndex Index of the rowtime or -1 if none
   * @param typeMapper
   * @param mapDirection
   */
  private void mapTypes(
      RelBuilder relBuilder,
      Sqrl2FlinkSQLTranslator sqrlEnv,
      int timestampIndex,
      DataTypeMapping typeMapper,
      DataTypeMapping.Direction mapDirection) {
    var sourceType = relBuilder.peek().getRowType();
    var hasChanged = false;
    var sourceFields = sourceType.getFieldList();
    List<RexNode> fields = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>();
    Preconditions.checkArgument(sourceType.getFieldCount() == sourceFields.size());
    for (var i = 0; i < sourceFields.size(); i++) {
      var field = sourceFields.get(i);
      if (mapDirection == Direction.FROM_DATABASE
          && field.getName().equalsIgnoreCase(HASHED_PK_NAME)) {
        // We ignore the appendex pk_hash field in order to preserve the original table schema
        hasChanged = true;
        continue;
      }
      fieldNames.add(field.getName());
      Optional<FunctionDefinition> castFct =
          typeMapper
              .getMapper(field.getType())
              .flatMap(mapper -> mapper.getEngineMapping(mapDirection));
      if (castFct.isPresent()) {
        // We need to cast the datatype
        var castFunction = castFct.get();
        hasChanged = true;
        fields.add(
            relBuilder
                .getRexBuilder()
                .makeCall(
                    sqrlEnv.lookupUserDefinedFunction(castFunction),
                    List.of(relBuilder.field(field.getIndex()))));
      } else if (mapDirection == Direction.TO_DATABASE
          && CalciteUtil.isRowTime(field.getType())
          && i != timestampIndex) {
        // We need to cast all other rowtime fields to their underlying type or Flink will complain
        var rowtimeType = (TimeIndicatorRelDataType) field.getType();
        var timestampType = rowtimeType.originalType();
        hasChanged = true;
        fields.add(
            relBuilder.getRexBuilder().makeCast(timestampType, relBuilder.field(field.getIndex())));
      } else {
        // Just select the field
        fields.add(relBuilder.field(i));
      }
    }

    if (hasChanged) {
      relBuilder.project(fields, fieldNames);
    }
  }

  public record InputTableKey(ExecutionStage stage, ObjectIdentifier tableId) {}

  /**
   * Expands the views in a query that are executed in the same execution stage.
   *
   * <p>TODO: handle sub-queries
   */
  @AllArgsConstructor
  private class QueryExpansionRelShuttle extends RelShuttleImpl {

    Function<ObjectIdentifier, ObjectIdentifier> inputTableMapping;
    Sqrl2FlinkSQLTranslator sqrlEnv;
    DataTypeMapping typeMapping;
    boolean addSort;

    @Override
    public RelNode visit(TableScan scan) {
      var table = scan.getTable();
      ObjectIdentifier tablePath;
      if (table instanceof TableSourceTable sourceTable) {
        tablePath = sourceTable.contextResolvedTable().getIdentifier();
      } else if (table instanceof FlinkPreparingTableBase preparingTable) {
        var names = preparingTable.getNames();
        tablePath = ObjectIdentifier.of(names.get(0), names.get(1), names.get(2));
      } else {
        throw new UnsupportedOperationException("Unexpected table: " + table.getClass());
      }
      var tableAnalysis = sqrlEnv.getTableLookup().lookupView(tablePath);
      errors.checkFatal(tableAnalysis != null, "Could not find table: %s", tablePath);

      var inputTableId = inputTableMapping.apply(tablePath);
      RelNode result;
      if (inputTableId != null) {
        var relBuilder = sqrlEnv.getTableScan(inputTableId);
        mapTypes(relBuilder, sqrlEnv, -1, typeMapping, Direction.FROM_DATABASE);
        result = relBuilder.build();
      } else {
        result = tableAnalysis.getCollapsedRelnode().accept(this);
      }
      if (addSort && tableAnalysis.getTopLevelSort().isPresent()) {
        Sort sort = tableAnalysis.getTopLevelSort().get();
        result = sort.copy(sort.getTraitSet(), List.of(result));
      }
      return result;
    }

    @Override
    public RelNode visit(TableFunctionScan scan) {
      var call = (RexCall) scan.getCall();
      if (call.getOperator() instanceof SqlUserDefinedTableFunction) {
        var tableFunction = ((SqlUserDefinedTableFunction) call.getOperator()).getFunction();
        if (tableFunction instanceof SqrlTableFunction sqrlFct) {
          var operands = ((RexCall) scan.getCall()).getOperands();
          var rewrittenNode = CalciteUtil.replaceParameters(sqrlFct.getRelNode(), operands);
          return rewrittenNode.accept(this);
        }
      }
      return super.visit(scan);
    }

    @Override
    public RelNode visit(RelNode other) {
      if (other instanceof LogicalTableFunctionScan scan) {
        return visit(scan);
      }
      return super.visit(other);
    }

    @Override
    public RelNode visit(LogicalProject project) {
      var input = project.getInput().accept(this);
      RexShuttle typeAdjustingShuttle = new ReplaceRowtimeRexShuttle(input.getRowType());

      List<Pair<RexNode, String>> newProjects =
          project.getNamedProjects().stream()
              .map(pair -> Pair.of(pair.left.accept(typeAdjustingShuttle), pair.right))
              .collect(Collectors.toList());
      var typeBuilder = CalciteUtil.getRelTypeBuilder(sqrlEnv.getTypeFactory());
      newProjects.forEach(pair -> typeBuilder.add(pair.right, pair.left.getType()));
      return project.copy(
          project.getTraitSet(),
          input,
          newProjects.stream().map(Pair::getKey).collect(Collectors.toList()),
          typeBuilder.build());
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
      var input = filter.getInput().accept(this);
      RexShuttle typeAdjustingShuttle = new ReplaceRowtimeRexShuttle(input.getRowType());
      return filter.copy(
          filter.getTraitSet(), input, filter.getCondition().accept(typeAdjustingShuttle));
    }

    @Override
    public RelNode visit(LogicalJoin join) {
      RelNode left = join.getLeft().accept(this), right = join.getRight().accept(this);
      RexShuttle typeAdjustingShuttle = new ReplaceRowtimeRexShuttle(null);
      return join.copy(
          join.getTraitSet(),
          join.getCondition().accept(typeAdjustingShuttle),
          left,
          right,
          join.getJoinType(),
          join.isSemiJoinDone());
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
      var input = aggregate.getInput().accept(this);
      return aggregate.copy(
          aggregate.getTraitSet(),
          input,
          aggregate.getGroupSet(),
          aggregate.groupSets,
          aggregate.getAggCallList().stream()
              .map(
                  agg ->
                      agg.adaptTo(
                          input,
                          agg.getArgList(),
                          agg.filterArg,
                          aggregate.getGroupCount(),
                          aggregate.getGroupCount()))
              .collect(Collectors.toList()));
    }

    /**
     * When producing the query RelNodes for database and server stages, we need to replace the
     * special ROWTIME datatype that is unique to Flink with a normal timestamp. This is implemented
     * in this RexShuttle.
     */
    @AllArgsConstructor
    private class ReplaceRowtimeRexShuttle extends RexShuttle {

      private final RelDataType newRowType;

      private RelDataType getNormalTimestampType(RelDataType type) {
        var timeIndicator = (TimeIndicatorRelDataType) type;
        return sqrlEnv
            .getTypeFactory()
            .createTypeWithNullability(timeIndicator.originalType(), type.isNullable());
      }

      @Override
      public RexNode visitInputRef(RexInputRef inputRef) {
        var type = inputRef.getType();
        if (CalciteUtil.isRowTime(type)) {
          return new RexInputRef(inputRef.getIndex(), getNormalTimestampType(type));
        } else if (newRowType != null) {
          var index = inputRef.getIndex();
          var newType = newRowType.getFieldList().get(index).getType();
          if (!inputRef.getType().equals(newType)) {
            return new RexInputRef(index, newType);
          }
        }
        return super.visitInputRef(inputRef);
      }

      @Override
      public RexNode visitCall(RexCall call) {
        if (CalciteUtil.isRowTime(call.getType())) {
          var update = new boolean[] {false};
          var clonedOperands = this.visitList(call.operands, update);
          return call.clone(getNormalTimestampType(call.getType()), clonedOperands);
        } else {
          return super.visitCall(call);
        }
      }
    }
  }
}
