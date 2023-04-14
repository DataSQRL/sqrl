/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational;

import static com.datasqrl.engine.EngineCapability.STANDARD_DATABASE;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.engine.database.relational.ddl.SqlDDLStatement;
import com.datasqrl.engine.database.relational.ddl.JdbcDDLFactory;
import com.datasqrl.engine.database.relational.ddl.JdbcDDLServiceLoader;
import com.datasqrl.io.DataSystemConnectorConfig;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.global.IndexSelectorConfig;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.tools.RelBuilder;

@Slf4j
public class JDBCEngine extends ExecutionEngine.Base implements DatabaseEngine {

//  public static final EnumMap<Dialect, EnumSet<EngineCapability>> CAPABILITIES_BY_DIALECT = new EnumMap<Dialect, EnumSet<EngineCapability>>(
//      Dialect.class);

//  static {
//    CAPABILITIES_BY_DIALECT.put(Dialect.POSTGRES, EnumSet.of(NOW, GLOBAL_SORT, MULTI_RANK));
//    CAPABILITIES_BY_DIALECT.put(Dialect.H2, EnumSet.of(NOW, GLOBAL_SORT, MULTI_RANK));
//  }

  @Getter
  final JDBCEngineConfiguration config;

  public JDBCEngine(JDBCEngineConfiguration configuration) {
    super(JDBCEngineConfiguration.ENGINE_NAME, Type.DATABASE, STANDARD_DATABASE);
//        CAPABILITIES_BY_DIALECT.get(configuration.getDialect()));
    this.config = configuration;
  }

  @Override
  public DataSystemConnectorConfig getDataSystemConnectorConfig() {
    return config.getConfig();
  }

  @Override
  public IndexSelectorConfig getIndexSelectorConfig() {
    return IndexSelectorConfigByDialect.of(config.getConfig().getDialect());
  }

  @Override
  public ExecutionResult execute(EnginePhysicalPlan plan) {
    Preconditions.checkArgument(plan instanceof JDBCPhysicalPlan);
    JDBCPhysicalPlan jdbcPlan = (JDBCPhysicalPlan) plan;
    List<String> dmls = jdbcPlan.getDdlStatements().stream().map(ddl -> ddl.toSql())
        .collect(Collectors.toList());
    try (Connection conn = DriverManager.getConnection(
        config.getConfig().getDbURL(),
        config.getConfig().getUser(),
        config.getConfig().getPassword())) {
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
    return new ExecutionResult.Message(
        String.format("Executed %d DDL statements", jdbcPlan.getDdlStatements().size()));
  }

  @Override
  public EnginePhysicalPlan plan(PhysicalDAGPlan.StagePlan plan, List<PhysicalDAGPlan.StageSink> inputs,
      RelBuilder relBuilder, TableSink errorSink) {

    JdbcDDLFactory factory =
        (new JdbcDDLServiceLoader()).load(config.getConfig().getDialect())
            .orElseThrow(() -> new RuntimeException("Could not find DDL factory"));

    List<SqlDDLStatement> ddlStatements = StreamUtil.filterByClass(inputs,
            EngineSink.class)
        .map(factory::createTable)
        .collect(Collectors.toList());

    plan.getIndexDefinitions().stream()
            .map(factory::createIndex)
            .forEach(ddlStatements::add);

    QueryBuilder queryBuilder = new QueryBuilder(relBuilder.getRexBuilder());
    Map<APIQuery, QueryTemplate> databaseQueries = queryBuilder.planQueries(plan.getQueries());
    return new JDBCPhysicalPlan(ddlStatements, databaseQueries);
  }
}