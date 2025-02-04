package com.datasqrl.engine.database.relational;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EmptyEngineConfig;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.database.DatabasePhysicalPlanOld;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.global.PhysicalDAGPlan.DatabaseStagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.NonNull;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

public class DuckDBEngine extends AbstractJDBCQueryEngine {

  @Inject
  public DuckDBEngine(
      @NonNull PackageJson json,
      ConnectorFactoryFactory connectorFactory) {
    super(DuckDBEngineFactory.ENGINE_NAME, json.getEngines().getEngineConfig(DuckDBEngineFactory.ENGINE_NAME)
            .orElseGet(()-> new EmptyEngineConfig(DuckDBEngineFactory.ENGINE_NAME)),
        connectorFactory);
  }

  @Override
  protected JdbcDialect getDialect() {
    return JdbcDialect.Postgres;
  }

  @Override
  protected JdbcStatementFactory getStatementFactory() {
    return new DuckDbStatementFactory(engineConfig);
  }


  @Override
  @Deprecated
  public DatabasePhysicalPlanOld plan(ConnectorFactoryFactory connectorFactory, EngineConfig connectorConfig,
      StagePlan plan, List<StageSink> inputs, ExecutionPipeline pipeline,
      List<StagePlan> stagePlans, SqrlFramework framework, ErrorCollector errorCollector) {
    Preconditions.checkArgument(plan instanceof DatabaseStagePlan);
    DatabaseStagePlan dbPlan = (DatabaseStagePlan) plan;
    DatabasePhysicalPlanOld physicalPlan = super.plan(plan, inputs, pipeline, stagePlans, framework, errorCollector);
    physicalPlan.removeIndexDdl();

    Map<IdentifiedQuery, QueryTemplate> databaseQueries = new LinkedHashMap<>();

    dbPlan.getQueries().forEach( readQuery -> {
      RelNode relNode = readQuery.getRelNode();

      RelNode replaced = relNode.accept(new RelShuttleImpl() {
        @Override
        public RelNode visit(TableScan scan) {
          Map<String, Object> map = connectorFactory.getConfig("iceberg").toMap();
          String warehouse =(String) map.get("warehouse");
          String databaseName =(String) map.get("database-name") == null ? "default_database" :
              (String)map.get("database-name");
          RexBuilder rexBuilder = new RexBuilder(new TypeFactory());
          if (warehouse.startsWith("file://")) {
            warehouse = warehouse.substring(7);
          }

          RexNode allowMovedPaths = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
              rexBuilder.makeFlag(DuckDbStatementFactory.Params.ALLOW_MOVED_PATHS),
              rexBuilder.makeLiteral(true));
          RexNode rexNode = rexBuilder.makeCall(lightweightOp("iceberg_scan"),
              rexBuilder.makeLiteral(warehouse+"/"+databaseName+"/" + scan.getTable().getQualifiedName().get(0)),
              allowMovedPaths);


          return new LogicalTableFunctionScan(scan.getCluster(), scan.getTraitSet(), List.of(), rexNode,
              Object.class, scan.getRowType(), Set.of());
        }
      });

      databaseQueries.put(readQuery.getQuery(), new QueryTemplate(getName(), replaced));
    });


    return new JDBCPhysicalPlanOld(physicalPlan.getDdl(), List.of(), databaseQueries);
  }


  @Override
  @Deprecated
  protected String createView(SqlIdentifier viewNameIdentifier, SqlParserPos pos,
      SqlNodeList columnList, SqlNode viewSqlNode) {
    //We currently don't support views in DuckDB and replace them with empty list in the #plan method above
    return "";
  }


}
