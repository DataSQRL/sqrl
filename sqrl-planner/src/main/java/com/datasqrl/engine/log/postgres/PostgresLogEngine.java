package com.datasqrl.engine.log.postgres;

import static com.datasqrl.config.EngineFactory.Type.LOG;
import static com.datasqrl.engine.log.postgres.PostgresLogEngineFactory.ENGINE_NAME;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

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
import com.datasqrl.engine.database.relational.ddl.JdbcDDLServiceLoader;
import com.datasqrl.engine.database.relational.ddl.PostgresDDLFactory;
import com.datasqrl.engine.database.relational.ddl.statements.InsertStatement;
import com.datasqrl.engine.database.relational.ddl.statements.notify.ListenNotifyAssets;
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

import lombok.Getter;

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
    this.sourceConnectorFactory = connectorFactory.create(LOG, "postgres_log-source")
        .orElseThrow(()->new RuntimeException("Could not find postgres_log source connector"));
    this.sinkConnectorFactory = connectorFactory.create(LOG, "postgres_log-sink")
        .orElseThrow(()->new RuntimeException("Could not find postgres_log sink connector"));
  }

  @Override
  public LogFactory getLogFactory() {
    return new PostgresLogFactory(sourceConnectorFactory, sinkConnectorFactory);
  }

  @Override
  public EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs, ExecutionPipeline pipeline,
      List<StagePlan> stagePlans, SqrlFramework framework, ErrorCollector errorCollector) {

    Preconditions.checkArgument(plan instanceof LogStagePlan);

    var factory = new JdbcDDLServiceLoader()
        .load(JdbcDialect.Postgres)
        .orElseThrow(() -> new RuntimeException("Could not find DDL factory"));

    var postgresDDLFactory = (PostgresDDLFactory) factory;

    List<SqlDDLStatement> ddl = new ArrayList<>();
    List<ListenNotifyAssets> queries = new ArrayList<>();
    List<InsertStatement> inserts = new ArrayList<>();
    var dbPlan = (LogStagePlan) plan;
    for (Log log : dbPlan.getLogs()) {
      var pgTable = (PostgresTable) log;
      var tableName = pgTable.getTableName();
      var dataType = pgTable.getTableSchema().getRelDataType();
      ddl.add(postgresDDLFactory.createTable(tableName, dataType.getFieldList(), pgTable.getPrimaryKeys()));
      ddl.add(postgresDDLFactory.createNotify(tableName, pgTable.getPrimaryKeys()));

      var listenNotifyAssets = postgresDDLFactory.createNotifyHelperDDLs(framework, tableName, dataType, pgTable.getPrimaryKeys());
      queries.add(listenNotifyAssets);

      var insertStatement = postgresDDLFactory.createInsertHelperDMLs(tableName, dataType);
      inserts.add(insertStatement);
    }

    return new PostgresLogPhysicalPlan(ddl, queries, inserts);
  }

}
