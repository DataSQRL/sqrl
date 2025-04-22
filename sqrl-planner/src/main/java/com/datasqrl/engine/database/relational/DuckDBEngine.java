package com.datasqrl.engine.database.relational;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import com.datasqrl.graphql.jdbc.DatabaseType;
import com.datasqrl.plan.global.PhysicalDAGPlan.DatabaseStagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import lombok.NonNull;

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
  protected DatabaseType getDatabaseType() {
    return DatabaseType.DUCKDB;
  }

  @Override
  public JdbcStatementFactory getStatementFactory() {
    return new DuckDbStatementFactory(engineConfig);
  }

}
