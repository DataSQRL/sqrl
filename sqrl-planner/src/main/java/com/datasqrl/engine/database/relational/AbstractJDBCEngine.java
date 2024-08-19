/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.engine.database.relational.ddl.JdbcDDLFactory;
import com.datasqrl.engine.database.relational.ddl.JdbcDDLServiceLoader;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.global.PhysicalDAGPlan.DatabaseStagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.ReadQuery;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.sql.PgExtension;
import com.datasqrl.sql.SqlDDLStatement;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

/**
 * This is an abstract engine implementation that provides shared functionalities for relational/jdbc-compatible
 * engines.
 *
 * It implements the physical plan construction by creating DDL statements and queries for the relational database dialect.
 */
@Slf4j
public abstract class AbstractJDBCEngine extends ExecutionEngine.Base implements ExecutionEngine {

  public AbstractJDBCEngine(@NonNull String name, @NonNull Type type,
      @NonNull EnumSet<EngineFeature> capabilities) {
    super(name, type, capabilities);
  }

  protected abstract JdbcDialect getDialect();

  @Override
  public EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs,
      ExecutionPipeline pipeline, List<StagePlan> stagePlans, SqrlFramework framework, ErrorCollector errorCollector) {

    Preconditions.checkArgument(plan instanceof DatabaseStagePlan);
    DatabaseStagePlan dbPlan = (DatabaseStagePlan) plan;
    JdbcDDLFactory factory =
        (new JdbcDDLServiceLoader()).load(getDialect())
            .orElseThrow(() -> new RuntimeException("Could not find DDL factory"));

    List<SqlDDLStatement> ddlStatements = new ArrayList<>();

    List<SqlDDLStatement> typeExtensions = extractTypeExtensions(dbPlan.getQueries());
    ddlStatements.addAll(typeExtensions);

    ddlStatements.addAll(
      StreamUtil.filterByClass(inputs,
            EngineSink.class)
        .map(factory::createTable)
        .collect(Collectors.toList()));

    dbPlan.getIndexDefinitions().stream().sorted()
            .map(factory::createIndex)
            .forEach(ddlStatements::add);

    Map<IdentifiedQuery, QueryTemplate> databaseQueries = dbPlan.getQueries().stream()
        .collect(Collectors.toMap(ReadQuery::getQuery, q -> new QueryTemplate(
            getDialect().name().toLowerCase(), q.getRelNode())));

//    List<Map<String, String>> queries = new ArrayList<>();
//    QueryPlanner queryPlanner = framework.getQueryPlanner();
//    for (QueryTemplate template : databaseQueries.values()) {
//      String sql = queryPlanner.relToString(mapDialect(getDialect()),
//          template.getRelNode())
//          .getSql();
//      //todo: migrate this to a full form for consumption by downstream engines
//      queries.add(Map.of("sql", sql + ";"));
//    }

    return new JDBCPhysicalPlan(ddlStatements, databaseQueries);
  }

  private List<SqlDDLStatement> extractTypeExtensions(List<ReadQuery> queries) {
    List<PgExtension> extensions = ServiceLoaderDiscovery.getAll(PgExtension.class);

    return queries.stream()
        .flatMap(relNode -> extractTypeExtensions(relNode.getRelNode(), extensions).stream())
        .distinct()
        .collect(Collectors.toList());
  }

  //todo: currently vector specific
  private List<SqlDDLStatement> extractTypeExtensions(RelNode relNode, List<PgExtension> extensions) {
    Set<SqlDDLStatement> statements = new HashSet<>();
    //look at relnodes to see if we use a vector type
    for (RelDataTypeField field : relNode.getRowType().getFieldList()) {

      for (PgExtension extension : extensions) {
        if (field.getType() instanceof RawRelDataType &&
            ((RawRelDataType) field.getType()).getRawType().getOriginatingClass()
                == extension.typeClass())
          statements.add(extension.getExtensionDdl());
      }
    }

    CalciteUtil.applyRexShuttleRecursively(relNode, new RexShuttle() {
      @Override
      public RexNode visitCall(RexCall call) {
        for (PgExtension extension : extensions) {
          for (String function : extension.operators()) {
            if (function.equals(
                Name.system(call.getOperator().getName().toLowerCase()))) {
              statements.add(extension.getExtensionDdl());
            }
          }
        }

        return super.visitCall(call);
      }
    });

    return new ArrayList<>(statements);
  }
}