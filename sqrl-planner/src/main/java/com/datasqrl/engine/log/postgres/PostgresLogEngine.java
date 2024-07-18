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
import com.datasqrl.engine.database.relational.ddl.JdbcDDLFactory;
import com.datasqrl.engine.database.relational.ddl.JdbcDDLServiceLoader;
import com.datasqrl.engine.database.relational.ddl.PostgresDDLFactory;
import com.datasqrl.engine.log.Log;
import com.datasqrl.engine.log.LogEngine;
import com.datasqrl.engine.log.LogFactory;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.global.PhysicalDAGPlan.LogStagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.sql.SqlDDLStatement;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;

public class PostgresLogEngine extends ExecutionEngine.Base implements LogEngine {

  @Getter
  private final EngineConfig engineConfig;

  private final ConnectorFactory sourceConnectorFactory;
  private final ConnectorFactory sinkConnectorFactory;

  @Inject
  public PostgresLogEngine(PackageJson json, ConnectorFactoryFactory connectorFactory) {
    super(ENGINE_NAME, LOG, EnumSet.noneOf(EngineFeature.class));

    this.engineConfig = json.getEngines().getEngineConfig(ENGINE_NAME)
        .orElseGet(() -> new EmptyEngineConfig(ENGINE_NAME));
    this.sourceConnectorFactory = connectorFactory.create(LOG, "postgres-log-source").orElse(null);
    this.sinkConnectorFactory = connectorFactory.create(LOG, "postgres-log-sink").orElse(null);
  }

  @Override
  public LogFactory getLogFactory() {
    return new PostgresLogFactory(sourceConnectorFactory, sinkConnectorFactory);
  }

  @Override
  public EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs, ExecutionPipeline pipeline,
      SqrlFramework framework, ErrorCollector errorCollector) {

    Preconditions.checkArgument(plan instanceof LogStagePlan);

    JdbcDDLFactory factory = new JdbcDDLServiceLoader()
        .load(JdbcDialect.Postgres)
        .orElseThrow(() -> new RuntimeException("Could not find DDL factory"));

    PostgresDDLFactory postgresDDLFactory = (PostgresDDLFactory) factory;

    List<SqlDDLStatement> ddlStatements = new ArrayList<>();

    LogStagePlan dbPlan = (LogStagePlan) plan;
    for (Log log : dbPlan.getLogs()) {
      PostgresTable pgTable = (PostgresTable) log;
      String topicName = pgTable.getTopicName();
      RelDataType dataType = pgTable.getTableSchema().getRelDataType();
      ddlStatements.add(postgresDDLFactory.createTable(topicName, dataType.getFieldList()));
      ddlStatements.add(postgresDDLFactory.createNotify(topicName, "_uuid"));
    }

    return new PostgresPhysicalPlan(ddlStatements);
  }

}
