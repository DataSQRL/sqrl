package com.datasqrl.plan.global;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.log.Log;
import com.datasqrl.engine.log.LogManager;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableType;
import com.datasqrl.plan.RelStageRunner;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.ExternalSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.StreamStagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StreamStagePlan.TableDefinition;
import com.datasqrl.plan.global.SqrlDAG.ExportNode;
import com.datasqrl.plan.global.SqrlDAG.SqrlNode;
import com.datasqrl.plan.global.SqrlDAG.TableNode;
import com.datasqrl.plan.hints.TimestampHint;
import com.datasqrl.plan.local.generate.QueryTableFunction;
import com.datasqrl.plan.rules.AnnotatedLP;
import com.datasqrl.plan.rules.SQRLConverter;
import com.datasqrl.plan.rules.SqrlConverterConfig;
import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.datasqrl.plan.table.PhysicalTable;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.calcite.SqrlRexUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.tools.RelBuilder;

import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.datasqrl.error.ErrorCode.PRIMARY_KEY_NULLABLE;
import static com.datasqrl.plan.OptimizationStage.*;

@AllArgsConstructor(onConstructor_=@Inject)
public class DAGAssembler {
  private final SqrlFramework framework;
  private final SQRLConverter sqrlConverter;
  private final ExecutionPipeline pipeline;
  private final LogManager logManager;
  private final ErrorCollector errors;

  public PhysicalDAGPlan assemble(SqrlDAG dag, Set<URL> jars) {
    //We make the assumption that there is a single stream stage
    ExecutionStage streamStage = pipeline.getStageByType(Type.STREAMS).orElseThrow();
    List<PhysicalDAGPlan.WriteQuery> streamQueries = new ArrayList<>();
    //We make the assumption that there is a single (optional) server stage
    Optional<ExecutionStage> serverStage = pipeline.getStageByType(Type.SERVER);
    List<PhysicalDAGPlan.ReadQuery> serverQueries = new ArrayList<>();

    //Plan API queries and find all tables that need to be materialized
    HashMultimap<ExecutionStage, DatabaseQuery> queriesByStage = HashMultimap.create();
    VisitTableScans serverScanVisitor = new VisitTableScans();
    dag.allNodesByClass(SqrlDAG.QueryNode.class).forEach( query -> {
      ExecutionStage stage = query.getChosenStage();
      AnalyzedAPIQuery apiQuery = query.getQuery();
      if (stage.getEngine().getType()==Type.SERVER) { //this must be the serverStage by assumption
        RelNode relNode = apiQuery.getRelNode(serverStage.get(), sqrlConverter, errors);
        relNode = RelStageRunner.runStage(SERVER_DAG_STITCHING, relNode, framework.getQueryPlanner().getPlanner());
        serverScanVisitor.findScans(relNode);
        serverQueries.add(new PhysicalDAGPlan.ReadQuery(apiQuery.getBaseQuery(), relNode));
      } else {
        queriesByStage.put(query.getChosenStage(), apiQuery);
      }
    });
    //Extract all tables and function from serverScanVisitor
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
            relNode = RelStageRunner.runStage(DATABASE_DAG_STITCHING, relNode, framework.getQueryPlanner().getPlanner());
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
        errors.checkFatal(materializedTable.getPrimaryKey().isDefined(), ErrorCode.TABLE_NOT_MATERIALIZE,"Table [%s] does not have a primary key and can therefore not be materialized", materializedTable);
        Preconditions.checkArgument(materializedTable.getTimestamp().size()<=1, "Should not have multiple timestamps at this point");
        errors.checkFatal(materializedTable.getType()== TableType.STATIC || materializedTable.getTimestamp().size()==1, ErrorCode.TABLE_NOT_MATERIALIZE, "Table [%s] does not have a timestamp and can therefore not be materialized", materializedTable);

        OptionalInt timestampIdx = materializedTable.getTimestamp().getOnlyCandidateOptional();
        RelNode processedRelnode = produceWriteTree(materializedTable.getPlannedRelNode(), timestampIdx);
        streamQueries.add(new PhysicalDAGPlan.WriteQuery(
            new EngineSink(materializedTable.getNameId(), materializedTable.getPrimaryKey().getPkIndexes(),
                materializedTable.getRowType(), timestampIdx, database),
            processedRelnode, materializedTable.getPlannedRelNode(), materializedTable.getType()));

      }

      //Third, pick index structures for materialized tables
      //Pick index structures for database tables based on the database queries
      IndexSelector indexSelector = new IndexSelector(framework,
          ((DatabaseEngine) database.getEngine()).getIndexSelectorConfig());
      Map<String, List<IndexDefinition>> indexHintsByTable = new LinkedHashMap<>();

      Collection<QueryIndexSummary> queryIndexSummaries = databaseQueries.stream().map(indexSelector::getIndexSelection)
          .flatMap(List::stream).collect(Collectors.toList());
      List<IndexDefinition> indexDefinitions = new ArrayList<>(indexSelector.optimizeIndexes(queryIndexSummaries)
          .keySet());
      materializedTables.forEach(table -> indexSelector.getIndexHints(table).ifPresent(indexHints -> {
        //First, remove all generated indexes for that table...
        indexDefinitions.removeIf(idx -> idx.getTableId().equals(table.getNameId()));
        //and overwrite with the specified ones
        indexDefinitions.addAll(indexHints);
      }));
      databasePlans.add(new PhysicalDAGPlan.DatabaseStagePlan(database, databaseQueries, indexDefinitions));
    }


    //Add exported tables
    dag.allNodesByClass(ExportNode.class).forEach(exportNode -> {
      Preconditions.checkArgument(exportNode.getChosenStage().equals(streamStage));
      AnalyzedExport export = exportNode.getExport();
      RelNode processedRelnode = produceWriteTree(export.getRelNode(),
          getExportBaseConfig().withStage(exportNode.getChosenStage()), errors);
      //Pick only the selected keys
      RelBuilder relBuilder1 = sqrlConverter.getRelBuilder().push(processedRelnode);
      if (export.getNumSelects().isPresent()) {
        int numFields2Select = export.getNumSelects().getAsInt();
        Preconditions.checkArgument(relBuilder1.peek().getRowType().getFieldCount()>=numFields2Select);
        relBuilder1.project(CalciteUtil.getIdentityRex(relBuilder1, numFields2Select));
      }
      processedRelnode = relBuilder1.build();
      ExternalSink externalSink = new ExternalSink(exportNode.getUniqueId(), export.getSink());
      streamQueries.add(new PhysicalDAGPlan.WriteQuery(externalSink,
          processedRelnode, export.getRelNode(), externalSink.getTableSink().getConfiguration().getConnectorConfig().getTableType()
      ));
    });
    //Add debugging output
//    AtomicInteger debugCounter = new AtomicInteger(0);
//    Lists.newArrayList(Iterables.filter(dag, SqrlDAG.TableNode.class)).stream()
//        .filter(node -> node.getChosenStage().equals(streamStage))
//        .map(SqrlDAG.TableNode::getTable).filter(tbl -> debugger.isDebugTable(tbl.getTableName()))
//        .sorted(Comparator.comparing(PhysicalTable::getTableName))
//        .forEach(table -> {
//          Name debugSinkName = table.getTableName().suffix("debug" + debugCounter.incrementAndGet());
//          TableSink sink = debugger.getDebugSink(debugSinkName, errors);
//          RelNode expandedRelNode = produceWriteTree(table.getPlannedRelNode(), table.getTimestamp().getOnlyCandidate());
//          streamQueries.add(new PhysicalDAGPlan.WriteQuery(
//              new PhysicalDAGPlan.ExternalSink(debugSinkName.getCanonical(), sink),
//              expandedRelNode));
//        });

    //Get all tables that are computed in the stream
    List<StreamStagePlan.TableDefinition> streamTables = new ArrayList<>();
    for (SqrlNode node : dag) { //Make sure we traverse the DAG from source to sink
      if (node instanceof TableNode) {
        PhysicalTable table = ((TableNode) node).getTable();
        streamTables.add(new TableDefinition(table.getNameId(), table.getPlannedRelNode()));
      }
    }
    PhysicalDAGPlan.StagePlan streamPlan = new StreamStagePlan(streamStage, streamQueries,
        streamTables, jars);

    //Collect all the stage plans
    List<PhysicalDAGPlan.StagePlan> allPlans = new ArrayList<>();
    allPlans.add(streamPlan);
    allPlans.addAll(databasePlans);

    if (serverStage.isPresent()) {
      PhysicalDAGPlan.StagePlan serverPlan = new PhysicalDAGPlan.ServerStagePlan(
          serverStage.get(), serverQueries);
      allPlans.add(serverPlan);
    }
    Optional<ExecutionStage> logStage = pipeline.getStageByType(Type.LOG);
    Map<String, Log> logs = logManager.getLogs();
    if (logStage.isPresent()) {
      List<Log> sortedLogs = logs.entrySet().stream()
          .sorted(Map.Entry.comparingByKey())
          .map(Map.Entry::getValue)
          .collect(Collectors.toList());
      PhysicalDAGPlan.StagePlan logPlan = new PhysicalDAGPlan.LogStagePlan(
          logStage.get(), sortedLogs);
      allPlans.add(logPlan);
    } else if (!logs.isEmpty()) {
      throw new IllegalStateException("Should not have logs when no log engine is present");
    }

    return new PhysicalDAGPlan(allPlans, pipeline);
  }

  public static SqrlConverterConfig getExportBaseConfig() {
    return SqrlConverterConfig.builder().build();
  }

  private RelNode produceWriteTree(RelNode relNode, SqrlConverterConfig config, ErrorCollector errors) {
    AnnotatedLP alp = sqrlConverter.convert(relNode, config, errors);
    RelNode convertedRelNode = alp.getRelNode();
    //Expand to full tree
    return produceWriteTree(convertedRelNode, alp.getTimestamp().getOnlyCandidateOptional());
  }

  private RelNode produceWriteTree(RelNode convertedRelNode, OptionalInt timestampIndex) {
    RelNode expandedRelNode = RelStageRunner.runStage(STREAM_DAG_STITCHING, convertedRelNode,
        framework.getQueryPlanner().getPlanner());
    if (timestampIndex.isPresent()) {
      TimestampHint timestampHint = new TimestampHint(timestampIndex.getAsInt());
      expandedRelNode = timestampHint.addHint((Hintable) expandedRelNode);
    }
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
