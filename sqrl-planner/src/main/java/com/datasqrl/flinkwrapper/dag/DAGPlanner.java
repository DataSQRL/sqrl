package com.datasqrl.flinkwrapper.dag;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.EngineType;
import com.datasqrl.datatype.DataTypeMapper;
import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.datatype.DataTypeMapping.Direction;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.engine.export.ExportEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.engine.server.ServerEngine;
import com.datasqrl.engine.stream.flink.connector.CastFunction;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.flinkwrapper.Sqrl2FlinkSQLTranslator;
import com.datasqrl.flinkwrapper.TableAnalysisLookup;
import com.datasqrl.flinkwrapper.analyzer.TableAnalysis;
import com.datasqrl.flinkwrapper.dag.nodes.ExportNode;
import com.datasqrl.flinkwrapper.dag.nodes.PipelineNode;
import com.datasqrl.flinkwrapper.dag.nodes.PlannedNode;
import com.datasqrl.flinkwrapper.dag.nodes.TableFunctionNode;
import com.datasqrl.flinkwrapper.dag.nodes.TableNode;
import com.datasqrl.flinkwrapper.dag.plan.MaterializationStagePlan;
import com.datasqrl.flinkwrapper.dag.plan.MaterializationStagePlan.MaterializationStagePlanBuilder;
import com.datasqrl.flinkwrapper.dag.plan.MaterializationStagePlan.Query;
import com.datasqrl.flinkwrapper.dag.plan.ServerStagePlan;
import com.datasqrl.flinkwrapper.hint.PartitionKeyHint;
import com.datasqrl.flinkwrapper.parser.AccessModifier;
import com.datasqrl.flinkwrapper.tables.AccessVisibility;
import com.datasqrl.flinkwrapper.tables.FlinkTableBuilder;
import com.datasqrl.flinkwrapper.tables.SqrlTableFunction;
import com.datasqrl.function.SqrlCastFunction;
import com.datasqrl.plan.global.PhysicalDAGPlan.WriteSink;
import com.datasqrl.plan.util.PrimaryKeyMap;
import com.datasqrl.util.CalciteUtil;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;

@AllArgsConstructor(onConstructor_=@Inject)
public class DAGPlanner {

  private final ExecutionPipeline pipeline;
  private final ErrorCollector errors;

  public PipelineDAG optimize(PipelineDAG dag) {
    dag = dag.trimToSinks();
    for (PipelineNode node : dag) {
      if (!node.hasViableStage()) {
        errors.fatal("Could not find execution stage for [%s]. Stage analysis below.\n%s",node.getName(), node.toString());
      }
    }
    try {
      dag.eliminateInviableStages(pipeline);
    } catch (PipelineDAG.NoPlanException ex) {
      //Print error message that is easy to read
      errors.fatal("Could not find execution stage for [%s]. Full DAG below.\n%s", ex.getNode().getName(), dag);
    }
    dag.forEach(node -> Preconditions.checkArgument(node.hasViableStage()));

    //Pick most cost-effective stage for each node and assign
    for (PipelineNode node : dag) {
      if (node.setCheapestStage()) {
        //If we eliminated stages, we make sure to eliminate all inviable stages
        dag.eliminateInviableStages(pipeline);
      }
      //Assign stage to table
      if (node instanceof TableNode) {
        TableAnalysis table = ((TableNode) node).getTableAnalysis();
        ExecutionStage stage = node.getChosenStage();
        Preconditions.checkNotNull(stage);
        //table.assignStage(stage); //this stage on the config below
      }
    }

    return dag;
  }

  public List<EnginePhysicalPlan> assemble(PipelineDAG dag, Sqrl2FlinkSQLTranslator sqrlEnv) {
    ExecutionStage streamStage = pipeline.getStageByType(EngineType.STREAMS).orElseThrow();
    Optional<ExecutionStage> serverStage = pipeline.getStageByType(EngineType.SERVER);
    List<ExecutionStage> dataStoreStages = pipeline.getStages().stream()
        .filter(stage -> stage.getEngine().getType().isDataStore()).collect(Collectors.toList());

    Map<ExecutionStage, MaterializationStagePlanBuilder> exportPlans = pipeline.getStages().stream()
        .filter(stage -> stage.getEngine().getType().supportsExport()).collect(Collectors.toMap(
            Function.identity(), s -> MaterializationStagePlan.builder().stage(s)));

    Optional<ExecutionStage> logStage = pipeline.getStageByType(EngineType.LOG);
    if (logStage.isPresent()) {
      //Add mutations that were planned during DAG building
      MaterializationStagePlanBuilder planBuilder = exportPlans.get(logStage.get());
      dag.allNodesByClass(TableNode.class).flatMap(node -> node.getMutation().stream()).
          forEach(planBuilder::mutation);
    }

    Map<InputTableKey, ObjectIdentifier> streamTableMapping = new HashMap<>();
    List<EnginePhysicalPlan> plans = new ArrayList<>();
    //move assembler logic here
    //1st: find all the cuts between flink and materialization (db+log) stages or sinks
    //generate a sink for each in the respective engine and insert into
    dag.allNodesByClassAndStage(TableNode.class, streamStage).forEach(node -> {
      ObjectIdentifier fromTableId = node.getIdentifier();
      TableAnalysis nodeTable = node.getTableAnalysis();
      Set<ExecutionStage> downstreamStages = new HashSet<>(); //We want to only plan once for each stage
      for (PipelineNode downstream : dag.getOutputs(node)) {
        if (downstream instanceof ExportNode || !downstream.getChosenStage().equals(streamStage)) {
          //Create sink
          ExecutionStage exportStage = null;
          FlinkRelBuilder relBuilder = sqrlEnv.getRelBuilder(null);
          ObjectIdentifier targetTable = null;
          if (downstream instanceof ExportNode) {
            ExportNode exportNode = (ExportNode) downstream;
            if (exportNode.getSinkTo().isPresent()) {
              exportStage = exportNode.getSinkTo().get();
            } else {
              //Special case, we sink directly to table
              targetTable = exportNode.getCreatedSinkTable().get();
              relBuilder.scan(node.getIdentifier(), Map.of());
            }
          } else {
            exportStage = downstream.getChosenStage();
            if (!downstreamStages.add(exportStage)) exportStage = null;
          }
          if (exportStage != null) {
            Preconditions.checkArgument(exportStage.getEngine().getType().supportsExport(), "Execution stage [%s] does not support exporting [%s]", exportStage, node);
            ExportEngine exportEngine = (ExportEngine) exportStage.getEngine();
            FlinkTableBuilder tblBuilder = new FlinkTableBuilder();

            //1st: determine primary key and partition key (if present)
            PrimaryKeyMap pk = deterinePrimaryKey(nodeTable, relBuilder, exportStage);
            if (pk.isDefined()) {
              List<RelDataTypeField> fields = relBuilder.peek().getRowType().getFieldList();
              List<String> pkColNames = pk.asSimpleList().stream().map(fields::get).map(RelDataTypeField::getName).collect(
                  Collectors.toList());
              tblBuilder.setPrimaryKey(pkColNames);
            }
            nodeTable.getHints().getHint(PartitionKeyHint.class).ifPresent(
                partitionKeyHint -> tblBuilder.setPartition(partitionKeyHint.getOptions()));

            //2nd: apply type casting
            mapTypes(relBuilder, nodeTable.getRowType(), sqrlEnv, exportEngine.getTypeMapping(), Direction.TO_ENGINE);

            //3rd: create table
            EngineCreateTable createdTable = exportEngine.createTable(tblBuilder, relBuilder.peek().getRowType());
            exportPlans.get(exportStage).table(createdTable);
            targetTable = sqrlEnv.createSinkTable(tblBuilder);
            streamTableMapping.put(new InputTableKey(exportStage, fromTableId), targetTable);
          }
          //Finally: add insert statement to sink into table
          sqrlEnv.insertInto(relBuilder.build(), targetTable);
        }
      }
    });
    plans.add(sqrlEnv.compilePlan());

    //2nd: for each materialization stage, find all cuts to server or sinks
    //generate queries for those
    Map<ObjectIdentifier, SqrlTableFunction> databaseTableMapping = new HashMap<>();
    ServerStagePlan.ServerStagePlanBuilder serverPlan = ServerStagePlan.builder();
    for (ExecutionStage dataStoreStage : dataStoreStages) {
      DatabaseEngine dbEngine = (DatabaseEngine) dataStoreStage.getEngine();
      MaterializationStagePlanBuilder dbPlan = exportPlans.get(dataStoreStage);
      dag.allNodesByClassAndStage(PlannedNode.class, dataStoreStage).forEach(node -> {

        SqrlTableFunction function = null;
        if (node instanceof TableFunctionNode) {
          function = ((TableFunctionNode) node).getFunction();
        } else {
          /*We also have to plan all TableNodes that have a downstream server consumer and convert
           those to tablefunctions of the same name as the table, so they can be executed in the server
          */
          TableNode tableNode = (TableNode) node;
          boolean isQueriedByServer = dag.getOutputs(tableNode).stream().anyMatch(output -> output.getChosenStage().getType()==EngineType.SERVER);
          if (isQueriedByServer) {
            ObjectIdentifier tableid = tableNode.getIdentifier();
            function = SqrlTableFunction.builder().functionAnalysis(tableNode.getAnalysis())
                .fullPath(NamePath.of(tableid.getObjectName()))
                .visibility(new AccessVisibility(AccessModifier.NONE
                    , false, true, true))
                .build();
            databaseTableMapping.put(tableid, function);
          }
        }
        if (function != null) {
          RelNode plannedRelNode = expandQuery(function.getFunctionAnalysis().getRelNode(), sqrlEnv.getRelBuilder(null),
              dbEngine.getTypeMapping(), sqrlEnv, id -> streamTableMapping.get(new InputTableKey(dataStoreStage, id)));
          dbPlan.query(new Query(function, plannedRelNode));
        }
      });
      plans.add(dbEngine.plan(dbPlan.build())); //this attaches the planned queries to the SqrlTableFunction
    }

    //3rd: build table functions for server stage
    if (serverStage.isPresent()) {
      Preconditions.checkArgument(dag.allNodesByClassAndStage(PipelineNode.class, serverStage.get()).count()==0, "We do not yet support server side query execution");
      /*For now, we do not support execution on the server, hence we just pass the functions through
      since they must all be planned already. In the future, we could have SqrlTableFunctions that require
      server-side execution. Those may directly follow the stream stage, so we have to create a SqrlTableFunction as indicated by the comment above.
      Need to update SqlScriptPlanner availableStages for server to be included.
       */
      ServerEngine serverEngine = (ServerEngine) serverStage.get().getEngine();
      plans.add(serverEngine.plan(serverPlan.build()));
    }

    return plans;
  }

  public static final String HASHED_PK_NAME = "__pk_hash";

  private PrimaryKeyMap deterinePrimaryKey(TableAnalysis table, FlinkRelBuilder relBuilder, ExecutionStage stage) {
    PrimaryKeyMap pk = table.getSimplePrimaryKey();
    int numCols = table.getRowType().getFieldCount();
    List<Integer> addHashColumn = null;
    if (pk.isUndefined()) {
      //Databases requires a primary key, see if we can create one
      if (stage.getType()==EngineType.DATABASE) {
        table.getErrors().checkFatal(table.getType().isStream(),
            "Could not determine primary key for table [%s]. Please add a primary key with the /*+primary_key(...) */ hint.",
            table.getIdentifier().asSummaryString());
        //Hash all columns as primary key
        addHashColumn = IntStream.range(0, numCols).boxed().collect(
            Collectors.toList());
      } else {
        return pk; //We don't have and don't need a pk
      }
    } else {
      boolean hasNullPk = pk.asSimpleList().stream().map(table::getField)
          .map(RelDataTypeField::getType).anyMatch(RelDataType::isNullable);
      if (hasNullPk && stage.supportsFeature(EngineFeature.REQUIRES_NOT_NULL_PRIMARY_KEY)) {
        addHashColumn = pk.asSimpleList();
      }
    }
    if (addHashColumn != null) {
      CalciteUtil.addColumn(relBuilder, relBuilder.getRexBuilder()
          .makeCall(FlinkSqlOperatorTable.SHA256, CalciteUtil.getIdentityRex(relBuilder, numCols)), HASHED_PK_NAME);
      return PrimaryKeyMap.of(List.of(numCols));
    } else {
      return pk;
    }
  }

  private void mapTypes(RelBuilder relBuilder, RelDataType sourceType, Sqrl2FlinkSQLTranslator sqrlEnv,
      DataTypeMapping typeMapper, DataTypeMapping.Direction mapDirection) {

    boolean hasChanged = false;
    List<RelDataTypeField> sourceFields = sourceType.getFieldList();
    List<RexNode> fields = new ArrayList<>();
    for (int i = 0; i < sourceFields.size(); i++) {
      if (i < sourceType.getFieldCount()) {
        RelDataTypeField field = sourceFields.get(i);
        Optional<SqrlCastFunction> castFct = typeMapper.getMapper(field.getType()).flatMap(mapper -> mapper.getEngineMapping(mapDirection));
        if (castFct.isPresent()) {
          SqrlCastFunction castFunction = castFct.get();
          hasChanged = true;
          fields.add(relBuilder.getRexBuilder()
              .makeCall(sqrlEnv.lookupUserDefinedFunction(castFunction), List.of(relBuilder.field(field.getIndex()))));
          continue;
        }
      }
      //Add reference to field
      fields.add(relBuilder.field(i));
    }

    if (hasChanged) {
      relBuilder.project(fields);
    }
  }

  public RelNode expandQuery(RelNode query, FlinkRelBuilder relBuilder, DataTypeMapping typeMapping,
      Sqrl2FlinkSQLTranslator sqrlEnv, Function<ObjectIdentifier, ObjectIdentifier> inputTableMapping) {
    return query.accept(new RelShuttleImpl() {
      @Override
      public RelNode visit(TableScan scan) {
        RelOptTable table = scan.getTable();
        Preconditions.checkArgument(table instanceof TableSourceTable);
        ObjectIdentifier tablePath = ((TableSourceTable)table).contextResolvedTable().getIdentifier();
        ObjectIdentifier inputTableId = inputTableMapping.apply(tablePath);
        if (inputTableId != null) {
          relBuilder.scan(inputTableId, Map.of());
          mapTypes(relBuilder, scan.getRowType(), sqrlEnv, typeMapping, Direction.FROM_ENGINE);
          return relBuilder.build();
        } else {
          TableAnalysis tableAnalysis = sqrlEnv.lookupSourceTable(tablePath);
          errors.checkFatal(tableAnalysis!=null, "Could not find table: %s", tablePath);
          return tableAnalysis.getCollapsedRelnode().accept(this);
        }
      }

      @Override
      public RelNode visit(TableFunctionScan scan) {
        RexCall call = (RexCall) scan.getCall();
        if (call.getOperator() instanceof SqlUserDefinedTableFunction) {
          TableFunction tableFunction = ((SqlUserDefinedTableFunction)call.getOperator()).getFunction();
          if (tableFunction instanceof SqrlTableFunction) {
            SqrlTableFunction sqrlFct = (SqrlTableFunction) tableFunction;
            List<RexNode> operands = ((RexCall)scan.getCall()).getOperands();
            RelNode rewrittenNode = CalciteUtil.replaceParameters(sqrlFct.getRelNode(), operands);
            return rewrittenNode.accept(this);
          }
        }
        return super.visit(scan);
      }
    });
  }



  @Value
  public static class InputTableKey {
    ExecutionStage stage;
    ObjectIdentifier tableId;
  }

}
