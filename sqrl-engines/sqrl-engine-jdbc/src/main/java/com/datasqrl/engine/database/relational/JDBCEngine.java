/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational;

import static com.datasqrl.engine.EngineCapability.STANDARD_DATABASE;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.engine.database.relational.ddl.PostgresCreateVectorExtensionStatement;
import com.datasqrl.engine.database.relational.ddl.SqlDDLStatement;
import com.datasqrl.engine.database.relational.ddl.JdbcDDLFactory;
import com.datasqrl.engine.database.relational.ddl.JdbcDDLServiceLoader;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.DataSystemConnectorFactory;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.formats.FormatFactory;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnectorFactory;
import com.datasqrl.io.tables.BaseTableConfig;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.global.IndexSelectorConfig;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.DatabaseStagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.ReadQuery;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JDBCEngine extends ExecutionEngine.Base implements DatabaseEngine {

//  public static final EnumMap<Dialect, EnumSet<EngineCapability>> CAPABILITIES_BY_DIALECT = new EnumMap<Dialect, EnumSet<EngineCapability>>(
//      Dialect.class);

//  static {
//    CAPABILITIES_BY_DIALECT.put(Dialect.POSTGRES, EnumSet.of(NOW, GLOBAL_SORT, MULTI_RANK));
//    CAPABILITIES_BY_DIALECT.put(Dialect.H2, EnumSet.of(NOW, GLOBAL_SORT, MULTI_RANK));
//  }

  @Getter
  final JdbcDataSystemConnector connector;

  public JDBCEngine(JdbcDataSystemConnector connector) {
    super(JDBCEngineFactory.ENGINE_NAME, Type.DATABASE, STANDARD_DATABASE);
//        CAPABILITIES_BY_DIALECT.get(configuration.getDialect()));
    this.connector = connector;
  }

  @Override
  public TableConfig getSinkConfig(String sinkName) {
    TableConfig.Builder tblBuilder = TableConfig.builder(sinkName)
            .base(BaseTableConfig.builder()
                    .type(ExternalDataType.sink.name())
                    .identifier(sinkName)
                    .build());
    SqrlConfig connectorConfig = tblBuilder.getConnectorConfig();
    connectorConfig.setProperty(DataSystemConnectorFactory.SYSTEM_NAME_KEY, JdbcDataSystemConnectorFactory.SYSTEM_NAME);
    connectorConfig.setProperties(connector);
    tblBuilder.getFormatConfig().setProperty(FormatFactory.FORMAT_NAME_KEY, JDBCFormat.FORMAT_NAME);
    return tblBuilder.build();
  }

  @Override
  public IndexSelectorConfig getIndexSelectorConfig() {
    return IndexSelectorConfigByDialect.of(connector.getDialect());
  }

  @Override
  public CompletableFuture<ExecutionResult> execute(EnginePhysicalPlan plan, ErrorCollector errors) {
    Preconditions.checkArgument(plan instanceof JDBCPhysicalPlan);
    JDBCPhysicalPlan jdbcPlan = (JDBCPhysicalPlan) plan;
    List<String> dmls = jdbcPlan.getDdlStatements().stream().map(ddl -> ddl.toSql())
        .collect(Collectors.toList());
    try (Connection conn = DriverManager.getConnection(
        connector.getUrl(),
        connector.getUser(),
        connector.getPassword())) {
      for (String dml : dmls) {
        try (Statement stmt = conn.createStatement()) {
          log.trace("Creating: " + dml);
          stmt.executeUpdate(dml);
        } catch (SQLException e) {
          throw new RuntimeException("Could not execute SQL query", e);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Could not connect to database", e);
    }
    ExecutionResult.Message result = new ExecutionResult.Message(
        String.format("Executed %d DDL statements", jdbcPlan.getDdlStatements().size()));
    return CompletableFuture.completedFuture(result);
  }

  @Override
  public EnginePhysicalPlan plan(PhysicalDAGPlan.StagePlan plan,
      List<PhysicalDAGPlan.StageSink> inputs, ExecutionPipeline pipeline, SqrlFramework framework,
      TableSink errorSink) {
    Preconditions.checkArgument(plan instanceof DatabaseStagePlan);
    DatabaseStagePlan dbPlan = (DatabaseStagePlan) plan;
    JdbcDDLFactory factory =
        (new JdbcDDLServiceLoader()).load(connector.getDialect())
            .orElseThrow(() -> new RuntimeException("Could not find DDL factory"));

    List<SqlDDLStatement> ddlStatements = StreamUtil.filterByClass(inputs,
            EngineSink.class)
        .map(factory::createTable)
        .collect(Collectors.toList());

    dbPlan.getIndexDefinitions().stream()
            .map(factory::createIndex)
            .forEach(ddlStatements::add);

    Map<IdentifiedQuery, QueryTemplate> databaseQueries = dbPlan.getQueries().stream()
        .collect(Collectors.toMap(ReadQuery::getQuery, q -> new QueryTemplate(q.getRelNode())));

    return new JDBCPhysicalPlan(ddlStatements, databaseQueries);
  }
}