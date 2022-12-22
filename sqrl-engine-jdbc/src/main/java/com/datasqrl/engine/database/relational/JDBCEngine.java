/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational;

import com.datasqrl.engine.EngineConfiguration;
import com.datasqrl.engine.database.relational.dialect.JdbcDDLFactory;
import com.datasqrl.engine.database.relational.dialect.JdbcDDLServiceLoader;
import com.datasqrl.plan.global.OptimizedDAG.EngineSink;
import com.datasqrl.util.StreamUtil;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.engine.database.relational.ddl.SqlDDLStatement;
import com.datasqrl.plan.global.IndexSelectorConfig;
import com.datasqrl.plan.global.OptimizedDAG;
import com.datasqrl.plan.queries.APIQuery;
import com.google.common.base.Preconditions;
import java.sql.DriverManager;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.tools.RelBuilder;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

import static com.datasqrl.engine.EngineCapability.*;

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
    super(JDBCEngineConfiguration.ENGINE_NAME, Type.DATABASE,
        EnumSet.of(NOW, GLOBAL_SORT, MULTI_RANK));
//        CAPABILITIES_BY_DIALECT.get(configuration.getDialect()));
    this.config = configuration;
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
  public EnginePhysicalPlan plan(OptimizedDAG.StagePlan plan, List<OptimizedDAG.StageSink> inputs,
      RelBuilder relBuilder) {

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