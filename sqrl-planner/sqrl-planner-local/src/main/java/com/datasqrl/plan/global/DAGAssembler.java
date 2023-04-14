package com.datasqrl.plan.global;

import static com.datasqrl.plan.OptimizationStage.READ_DAG_STITCHING;
import static com.datasqrl.plan.OptimizationStage.WRITE_DAG_STITCHING;

import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.plan.RelStageRunner;
import com.datasqrl.plan.rules.AnnotatedLP;
import com.datasqrl.plan.rules.SQRLConverter;
import com.datasqrl.plan.table.AbstractRelationalTable;
import com.datasqrl.plan.table.ScriptRelationalTable;
import com.datasqrl.plan.table.VirtualRelationalTable;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.plan.global.SqrlDAG.ExportNode;
import com.datasqrl.plan.global.SqrlDAG.QueryNode;
import com.datasqrl.plan.local.generate.Debugger;
import com.datasqrl.plan.local.generate.ResolvedExport;
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
import lombok.Value;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.table.functions.UserDefinedFunction;

@Value
public class DAGAssembler {

  private final RelOptPlanner planner;
  private final SQRLConverter sqrlConverter;
  private final ExecutionPipeline pipeline;

  private final Debugger debugger;

  private ErrorCollector errors;


  public PhysicalDAGPlan assemble(SqrlDAG dag, Set<URL> jars, Map<String, UserDefinedFunction> udfs) {
    //Plan final version of all tables
    dag.allNodesByClass(SqrlDAG.TableNode.class).forEach( tableNode -> {
      ExecutionStage stage = tableNode.getChosenStage();
      Preconditions.checkNotNull(stage);
      ScriptRelationalTable table = tableNode.getTable();
      table.setAssignedStage(Optional.of(stage)); //this stage on the config below
      SQRLConverter.Config config = table.getBaseConfig().build();
      table.setConvertedRelNode(sqrlConverter.convert(table, config, errors));
    });
    //We make the assumption that there is a single stream stage
    ExecutionStage streamStage = pipeline.getStage(Type.STREAM).get();

    //Plan API queries and find all tables that need to be materialized
    HashMultimap<ExecutionStage, SqrlDAG.QueryNode> queriesByStage = HashMultimap.create();
    dag.allNodesByClass(SqrlDAG.QueryNode.class).forEach( query -> {
      queriesByStage.put(query.getChosenStage(), query);
    });

    List<PhysicalDAGPlan.StagePlan> databasePlans = new ArrayList<>();
    List<PhysicalDAGPlan.WriteQuery> writeDAG = new ArrayList<>();

    //We want to preserve pipeline order in our iteration
    for (ExecutionStage database : Iterables.filter(pipeline.getStages(),queriesByStage::containsKey)) {
      Preconditions.checkArgument(database.getEngine().getType() == Type.DATABASE);
      List<PhysicalDAGPlan.ReadQuery> readDAG = new ArrayList<>();

      VisitTableScans tableScanVisitor = new VisitTableScans();
      queriesByStage.get(database).stream().sorted(Comparator.comparing(QueryNode::getName))
          .forEach(query -> {
        RelNode relNode = sqrlConverter.convert(query.getQuery(), query.getChosenStage(), errors);
        relNode = RelStageRunner.runStage(READ_DAG_STITCHING, relNode, planner);
        tableScanVisitor.findScans(relNode);
        readDAG.add(new PhysicalDAGPlan.ReadQuery(query.getQuery().getBaseQuery(), relNode));
      });

      Set<AbstractRelationalTable> materializedTables = tableScanVisitor.scanTables;
      List<VirtualRelationalTable> normalizedTables = StreamUtil.filterByClass(materializedTables,
          VirtualRelationalTable.class).sorted().collect(Collectors.toList());
      List<ScriptRelationalTable> denormalizedTables = StreamUtil.filterByClass(materializedTables,
          ScriptRelationalTable.class).sorted().collect(Collectors.toList());

      //Fill all table sinks
      //First, all the tables that need to be written to the database in normalized form
      for (VirtualRelationalTable normTable : normalizedTables) {
        RelNode scanTable = sqrlConverter.getRelBuilder().scan(normTable.getNameId()).build();
        SQRLConverter.Config.ConfigBuilder configBuilder = normTable.getRoot().getBase().getBaseConfig();
        configBuilder.fieldNames(normTable.getRowType().getFieldNames());
        Pair<RelNode, Integer> relPlusTimestamp = produceWriteTree(scanTable,
            configBuilder.build(), errors);
        RelNode processedRelnode = relPlusTimestamp.getKey();
        Preconditions.checkArgument(normTable.getRowType().equals(processedRelnode.getRowType()),
            "Rowtypes do not match: \n%s \n vs \n%s",
            normTable.getRowType(), processedRelnode.getRowType());
        writeDAG.add(new PhysicalDAGPlan.WriteQuery(
            new EngineSink(normTable.getNameId(), normTable.getNumPrimaryKeys(),
                normTable.getRowType(), relPlusTimestamp.getRight(), database),
            processedRelnode));
      }
      //Second, all tables that need to be written in denormalized form
      for (ScriptRelationalTable denormTable : denormalizedTables) {
        writeDAG.add(new PhysicalDAGPlan.WriteQuery(
            new EngineSink(denormTable.getNameId(), denormTable.getNumPrimaryKeys(),
                denormTable.getRowType(),
                denormTable.getTimestamp().getTimestampCandidate().getIndex(), database),
            denormTable.getConvertedRelNode()));
      }

      //Third, pick index structures for materialized tables
      //Pick index structures for database tables based on the database queries
      IndexSelector indexSelector = new IndexSelector(planner,
          ((DatabaseEngine) database.getEngine()).getIndexSelectorConfig());
      Collection<IndexCall> indexCalls = readDAG.stream().map(indexSelector::getIndexSelection)
          .flatMap(List::stream).collect(Collectors.toList());
      Collection<IndexDefinition> indexDefinitions = indexSelector.optimizeIndexes(indexCalls)
          .keySet();
      databasePlans.add(new PhysicalDAGPlan.StagePlan(database, readDAG, indexDefinitions,
          null, null));
    }

    //Add exported tables
    dag.allNodesByClass(ExportNode.class).forEach(exportNode -> {
      Preconditions.checkArgument(exportNode.getChosenStage().equals(streamStage));
      ResolvedExport export = exportNode.getExport();
      Pair<RelNode,Integer> relPlusTimestamp = produceWriteTree(export.getRelNode(),
          export.getBaseConfig().withStage(exportNode.getChosenStage()), errors);
      RelNode processedRelnode = relPlusTimestamp.getKey();
      //Pick only the selected keys
      RelBuilder relBuilder1 = sqrlConverter.getRelBuilder().push(processedRelnode);
      relBuilder1.project(export.getRelNode().getRowType().getFieldNames().stream()
          .map(n -> relBuilder1.field(n)).collect(Collectors.toList()));
      processedRelnode = relBuilder1.build();
      writeDAG.add(new PhysicalDAGPlan.WriteQuery(
          new PhysicalDAGPlan.ExternalSink(exportNode.getUniqueId(), export.getSink()),
          processedRelnode));
    });
    //Add debugging output
    AtomicInteger debugCounter = new AtomicInteger(0);
    Lists.newArrayList(Iterables.filter(dag, SqrlDAG.TableNode.class)).stream()
        .filter(node -> node.getChosenStage().equals(streamStage))
        .map(SqrlDAG.TableNode::getTable).filter(tbl -> debugger.isDebugTable(tbl.getTableName()))
        .sorted(Comparator.comparing(ScriptRelationalTable::getTableName))
        .forEach(table -> {
          Name debugSinkName = table.getTableName().suffix("debug" + debugCounter.incrementAndGet());
          TableSink sink = debugger.getDebugSink(debugSinkName, errors);
          RelNode convertedRelNode = table.getConvertedRelNode();
          RelNode expandedRelNode = RelStageRunner.runStage(WRITE_DAG_STITCHING, convertedRelNode, planner);
          writeDAG.add(new PhysicalDAGPlan.WriteQuery(
              new PhysicalDAGPlan.ExternalSink(debugSinkName.getCanonical(), sink),
              expandedRelNode));
        });

    PhysicalDAGPlan.StagePlan streamPlan = new PhysicalDAGPlan.StagePlan(streamStage, writeDAG,
        null, jars, udfs);

    return new PhysicalDAGPlan(ListUtils.union(List.of(streamPlan),databasePlans));

  }

  private Pair<RelNode,Integer> produceWriteTree(RelNode relNode, SQRLConverter.Config config, ErrorCollector errors) {
    Preconditions.checkArgument(config.isAddTimestamp2NormalizedChildTable());
    AnnotatedLP alp = sqrlConverter.convert(relNode, config, errors);
    RelNode convertedRelNode = alp.getRelNode();
    //Expand to full tree
    RelNode expandedRelNode = RelStageRunner.runStage(WRITE_DAG_STITCHING, convertedRelNode, planner);
    return Pair.of(expandedRelNode,alp.timestamp.getTimestampCandidate().getIndex());
  }


  /**
   *
   * This class is not thread safe
   */
  private static class VisitTableScans extends RelShuttleImpl {

    final Set<AbstractRelationalTable> scanTables = new HashSet<>();

    public void findScans(RelNode relNode) {
      relNode.accept(this);
    }

    @Override
    public RelNode visit(TableScan scan) {
      ScriptRelationalTable table = scan.getTable().unwrap(ScriptRelationalTable.class);
      if (table == null) { //It's a normalized query
        VirtualRelationalTable vtable = scan.getTable().unwrap(VirtualRelationalTable.class);
        Preconditions.checkNotNull(vtable);
        scanTables.add(vtable);
      } else {
        scanTables.add(table);
      }
      return super.visit(scan);
    }
  }


}
