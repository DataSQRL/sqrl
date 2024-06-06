package com.datasqrl.engine.log.postgres;

import static com.datasqrl.config.EngineFactory.Type.LOG;
import static com.datasqrl.engine.log.postgres.PostgresLogEngineFactory.ENGINE_NAME;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.config.ConnectorFactory;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EmptyEngineConfig;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.engine.database.relational.JDBCPhysicalPlan;
import com.datasqrl.engine.database.relational.ddl.JdbcDDLFactory;
import com.datasqrl.engine.database.relational.ddl.JdbcDDLServiceLoader;
import com.datasqrl.engine.log.LogEngine;
import com.datasqrl.engine.log.LogFactory;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.global.PhysicalDAGPlan.DatabaseStagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.LogStagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.ReadQuery;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.sql.SqlDDLStatement;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.commons.collections.ListUtils;

public class PostgresLogEngine extends ExecutionEngine.Base implements LogEngine {

  @Getter
  private final EngineConfig engineConfig;

  private final ConnectorFactory connectorFactory;

  @Inject
  public PostgresLogEngine(PackageJson json, ConnectorFactoryFactory connectorFactory) {
    super(ENGINE_NAME, LOG, EnumSet.noneOf(EngineFeature.class));

    this.engineConfig = json.getEngines().getEngineConfig(ENGINE_NAME)
        .orElseGet(() -> new EmptyEngineConfig(ENGINE_NAME));
    this.connectorFactory = connectorFactory.create(LOG, ENGINE_NAME).orElse(null);
  }

  @Override
  public LogFactory getLogFactory() {
    return new PostgresLogFactory(connectorFactory);
  }

  @Override
  public EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs, ExecutionPipeline pipeline,
      SqrlFramework relBuilder, ErrorCollector errorCollector) {

    Preconditions.checkArgument(plan instanceof LogStagePlan);

    LogStagePlan dbPlan = (LogStagePlan) plan;
    JdbcDDLFactory factory =
        (new JdbcDDLServiceLoader()).load(JdbcDialect.Postgres)
            .orElseThrow(() -> new RuntimeException("Could not find DDL factory"));

    List<SqlDDLStatement> ddlStatements = StreamUtil.filterByClass(inputs,
            EngineSink.class)
        .map(factory::createTable)
        .collect(Collectors.toList());

//    List<SqlDDLStatement> typeExtensions = extractTypeExtensions(dbPlan.getQueries());
//
//    ddlStatements = ListUtils.union(typeExtensions, ddlStatements);
//
//    dbPlan.getIndexDefinitions().stream().sorted()
//        .map(factory::createIndex)
//        .forEach(ddlStatements::add);
//
//    Map<IdentifiedQuery, QueryTemplate> databaseQueries = dbPlan.getQueries().stream()
//        .collect(Collectors.toMap(ReadQuery::getQuery, q -> new QueryTemplate(q.getRelNode())));

    return new PostgresPhysicalPlan(ddlStatements);
  }
}
