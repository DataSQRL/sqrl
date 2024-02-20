package com.datasqrl.plan.global;

import static com.datasqrl.error.ErrorCode.PRIMARY_KEY_NULLABLE;
import static com.datasqrl.plan.OptimizationStage.DATABASE_DAG_STITCHING;
import static com.datasqrl.plan.OptimizationStage.SERVER_DAG_STITCHING;
import static com.datasqrl.plan.OptimizationStage.STREAM_DAG_STITCHING;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.plan.RelStageRunner;
import com.datasqrl.plan.hints.TimestampHint;
import com.datasqrl.plan.local.generate.QueryTableFunction;
import com.datasqrl.plan.rules.AnnotatedLP;
import com.datasqrl.plan.rules.SQRLConverter;
import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.datasqrl.plan.table.PhysicalTable;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.plan.global.SqrlDAG.ExportNode;
import com.datasqrl.plan.local.generate.Debugger;
import com.datasqrl.plan.local.generate.ResolvedExport;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.SqrlRexUtil;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.table.functions.UserDefinedFunction;

@Value
public class DAGAssembler {

  private final SqrlFramework framework;
  private final RelOptPlanner planner;
  private final SQRLConverter sqrlConverter;
  private final ExecutionPipeline pipeline;

  private final Debugger debugger;

  private ErrorCollector errors;



  public PhysicalDAGPlan assemble(SqrlDAG dag, Set<URL> jars, Map<String, UserDefinedFunction> udfs,
      RootGraphqlModel model, APIConnectorManager apiManager) {
    //Plan final version of all tables
    dag.allNodesByClass(SqrlDAG.TableNode.class).forEach( tableNode -> {
      ExecutionStage stage = tableNode.getChosenStage();
      Preconditions.checkNotNull(stage);
      PhysicalTable table = tableNode.getTable();
      table.assignStage(stage); //this stage on the config below
      SQRLConverter.Config config = table.getBaseConfig().build();
      table.setPlannedRelNode(sqrlConverter.convert(table, config, errors));
    });

    //We make the assumption that there is a single stream stage
    ExecutionStage streamStage = pipeline.getStage(Type.STREAM).get();
    List<PhysicalDAGPlan.WriteQuery> streamQueries = new ArrayList<>();
    //We make the assumption that there is a single (optional) server stage
    Optional<ExecutionStage> serverStage = pipeline.getStage(Type.SERVER);
    List<PhysicalDAGPlan.ReadQuery> serverQueries = new ArrayList<>();

    //Plan API queries and find all tables that need to be materialized
    HashMultimap<ExecutionStage, DatabaseQuery> queriesByStage = HashMultimap.create();
    VisitTableScans serverScanVisitor = new VisitTableScans();
    dag.allNodesByClass(SqrlDAG.QueryNode.class).forEach( query -> {
      ExecutionStage stage = query.getChosenStage();
      AnalyzedAPIQuery apiQuery = query.getQuery();
      if (stage.getEngine().getType()==Type.SERVER) { //this must be the serverStage by assumption
        RelNode relNode = apiQuery.getRelNode(serverStage.get(), sqrlConverter, errors);
        relNode = RelStageRunner.runStage(SERVER_DAG_STITCHING, relNode, planner);
        serverScanVisitor.findScans(relNode);
        serverQueries.add(new PhysicalDAGPlan.ReadQuery(apiQuery.getBaseQuery(), relNode));
      } else {
        queriesByStage.put(query.getChosenStage(), apiQuery);
      }
    });
    //Extract all tables and function for serverScanVisitor
    Stream.concat(serverScanVisitor.scanTables.stream().map(DatabaseQuery::of),
        serverScanVisitor.scanFunctions.stream().map(DatabaseQuery::of))
        .forEach(q -> queriesByStage.put(q.getAssignedStage(), q));

    List<PhysicalDAGPlan.StagePlan> databasePlans = new ArrayList<>();

    //We want to preserve pipeline order in our iteration
    for (ExecutionStage database : Iterables.filter(pipeline.getStages(),queriesByStage::containsKey)) {
      Preconditions.checkArgument(database.getEngine().getType() == Type.DATABASE);
      List<PhysicalDAGPlan.ReadQuery> databaseQueries = new ArrayList<>();

      VisitTableScans tableScanVisitor = new VisitTableScans();
      queriesByStage.get(database).stream().sorted(Comparator.comparing(DatabaseQuery::getName))
          .forEach(query -> {
        RelNode relNode = query.getRelNode(database, sqrlConverter, errors);
        relNode = RelStageRunner.runStage(DATABASE_DAG_STITCHING, relNode, planner);
        tableScanVisitor.findScans(relNode);
        databaseQueries.add(new PhysicalDAGPlan.ReadQuery(query.getQueryId(), relNode));
      });

      Preconditions.checkArgument(tableScanVisitor.scanFunctions.isEmpty(),
          "Should not encounter table functions in materialized queries");
      List<PhysicalRelationalTable> materializedTables = tableScanVisitor.scanTables.stream()
              .sorted().collect(Collectors.toList());

      //Second, all tables that need to be written in denormalized form
      for (PhysicalRelationalTable materializedTable : materializedTables) {
        List<String> nullablePks = CalciteUtil.identifyNullableFields(materializedTable.getRowType(), materializedTable.getPrimaryKey().asList());
        errors.checkFatal(nullablePks.isEmpty(), PRIMARY_KEY_NULLABLE, "Cannot materialize table [%s] with nullable primary key: %s", materializedTable, nullablePks);
        errors.checkFatal(materializedTable.getPrimaryKey().isDefined(), ErrorCode.TALBE_NOT_MATERIALIZE,"Table [%s] does not have a primary key and can therefore not be materialized", materializedTable);
        errors.checkFatal(materializedTable.getTimestamp().hasCandidates(), ErrorCode.TALBE_NOT_MATERIALIZE, "Table [%s] does not have a timestamp and can therefore not be materialized", materializedTable);
        RelNode processedRelnode = produceWriteTree(materializedTable.getPlannedRelNode(),
                materializedTable.getTimestamp().getOnlyCandidate());
        streamQueries.add(new PhysicalDAGPlan.WriteQuery(
            new EngineSink(materializedTable.getNameId(), materializedTable.getPrimaryKey().getPkIndexes(),
                materializedTable.getRowType(),
                materializedTable.getTimestamp().getOnlyCandidate(), database),
            processedRelnode));
      }

      //Third, pick index structures for materialized tables
      //Pick index structures for database tables based on the database queries
      IndexSelector indexSelector = new IndexSelector(framework,
          ((DatabaseEngine) database.getEngine()).getIndexSelectorConfig());
      Collection<QueryIndexSummary> queryIndexSummaries = databaseQueries.stream().map(indexSelector::getIndexSelection)
          .flatMap(List::stream).collect(Collectors.toList());
      Collection<IndexDefinition> indexDefinitions = indexSelector.optimizeIndexes(queryIndexSummaries)
          .keySet();
      databasePlans.add(new PhysicalDAGPlan.DatabaseStagePlan(database, databaseQueries, indexDefinitions));
    }

    //Add exported tables
    dag.allNodesByClass(ExportNode.class).forEach(exportNode -> {
      Preconditions.checkArgument(exportNode.getChosenStage().equals(streamStage));
      ResolvedExport export = exportNode.getExport();
      RelNode processedRelnode = produceWriteTree(export.getRelNode(),
          getExportBaseConfig().withStage(exportNode.getChosenStage()), errors);
      //Pick only the selected keys
      RelBuilder relBuilder1 = sqrlConverter.getRelBuilder().push(processedRelnode);
      Preconditions.checkArgument(relBuilder1.peek().getRowType().getFieldCount()>=export.getNumSelects());
      relBuilder1.project(CalciteUtil.getIdentityRex(relBuilder1, export.getNumSelects()));
      processedRelnode = relBuilder1.build();
      streamQueries.add(new PhysicalDAGPlan.WriteQuery(
          new PhysicalDAGPlan.ExternalSink(exportNode.getUniqueId(), export.getSink()),
          processedRelnode));
    });
    //Add debugging output
    AtomicInteger debugCounter = new AtomicInteger(0);
    Lists.newArrayList(Iterables.filter(dag, SqrlDAG.TableNode.class)).stream()
        .filter(node -> node.getChosenStage().equals(streamStage))
        .map(SqrlDAG.TableNode::getTable).filter(tbl -> debugger.isDebugTable(tbl.getTableName()))
        .sorted(Comparator.comparing(PhysicalTable::getTableName))
        .forEach(table -> {
          Name debugSinkName = table.getTableName().suffix("debug" + debugCounter.incrementAndGet());
          TableSink sink = debugger.getDebugSink(debugSinkName, errors);
          RelNode expandedRelNode = produceWriteTree(table.getPlannedRelNode(), table.getTimestamp().getOnlyCandidate());
          streamQueries.add(new PhysicalDAGPlan.WriteQuery(
              new PhysicalDAGPlan.ExternalSink(debugSinkName.getCanonical(), sink),
              expandedRelNode));
        });

    PhysicalDAGPlan.StagePlan streamPlan = new PhysicalDAGPlan.StreamStagePlan(streamStage, streamQueries,
        jars, udfs);

    //Collect all the stage plans
    List<PhysicalDAGPlan.StagePlan> allPlans = new ArrayList<>();
    allPlans.add(streamPlan);
    allPlans.addAll(databasePlans);

    if (serverStage.isPresent()) {
      PhysicalDAGPlan.StagePlan serverPlan = new PhysicalDAGPlan.ServerStagePlan(
          serverStage.get(), model, serverQueries);
      allPlans.add(serverPlan);
    }
    Optional<ExecutionStage> logStage = pipeline.getStage(Type.LOG);
    if (logStage.isPresent()) {
      PhysicalDAGPlan.StagePlan logPlan = new PhysicalDAGPlan.LogStagePlan(
          logStage.get(), apiManager.getLogs());
      allPlans.add(logPlan);
    }

    return new PhysicalDAGPlan(allPlans, pipeline);
  }

  public static SQRLConverter.Config  getExportBaseConfig() {
      return SQRLConverter.Config.builder().build();
  }

  private RelNode produceWriteTree(RelNode relNode, SQRLConverter.Config config, ErrorCollector errors) {
    AnnotatedLP alp = sqrlConverter.convert(relNode, config, errors);
    RelNode convertedRelNode = alp.getRelNode();
    //Expand to full tree
    return produceWriteTree(convertedRelNode, alp.getTimestamp().getOnlyCandidate());
  }

  private RelNode produceWriteTree(RelNode convertedRelNode, int timestampIndex) {
    RelNode expandedRelNode = RelStageRunner.runStage(STREAM_DAG_STITCHING, convertedRelNode, planner);
    TimestampHint timestampHint = new TimestampHint(timestampIndex);
    expandedRelNode = timestampHint.addHint((Hintable) expandedRelNode);
    return expandedRelNode;
  }


  /**
   *
   * This class is not thread safe
   */
  private static class VisitTableScans extends RelShuttleImpl {

    final Set<PhysicalRelationalTable> scanTables = new HashSet<>();
    final Set<QueryTableFunction> scanFunctions = new HashSet<>();

    public void findScans(RelNode relNode) {
      relNode.accept(this);
    }

    @Override
    public RelNode visit(TableScan scan) {
      PhysicalRelationalTable table = scan.getTable().unwrap(PhysicalRelationalTable.class);
      Preconditions.checkArgument(table!=null, "Encountered unexpected table: %s", scan);
      scanTables.add(table);
      return super.visit(scan);
    }

    @Override
    public RelNode visit(TableFunctionScan scan) {
      SqrlRexUtil.getCustomTableFunction(scan).filter(QueryTableFunction.class::isInstance)
          .map(QueryTableFunction.class::cast).ifPresent(scanFunctions::add);
      return super.visit(scan);
    }
  }




}
