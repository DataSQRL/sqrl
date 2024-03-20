/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational;

import static com.datasqrl.engine.EngineFeature.STANDARD_DATABASE;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.sql.PgExtension;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.sql.SqlDDLStatement;
import com.datasqrl.engine.database.relational.ddl.JdbcDDLFactory;
import com.datasqrl.engine.database.relational.ddl.JdbcDDLServiceLoader;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.plan.global.IndexSelectorConfig;
import com.datasqrl.plan.global.PhysicalDAGPlan.DatabaseStagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.ReadQuery;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.type.JdbcTypeSerializer;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.FunctionUtil;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import java.util.*;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.commons.collections.ListUtils;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

@Slf4j
public class JDBCEngine extends ExecutionEngine.Base implements DatabaseEngine {

//  public static final EnumMap<Dialect, EnumSet<EngineCapability>> CAPABILITIES_BY_DIALECT = new EnumMap<Dialect, EnumSet<EngineCapability>>(
//      Dialect.class);

//  static {
//    CAPABILITIES_BY_DIALECT.put(Dialect.POSTGRES, EnumSet.of(NOW, GLOBAL_SORT, MULTI_RANK));
//    CAPABILITIES_BY_DIALECT.put(Dialect.H2, EnumSet.of(NOW, GLOBAL_SORT, MULTI_RANK));
//  }

  final SqrlConfig connectorConfig;

  @Getter
  final JdbcDataSystemConnector connector;

  final JdbcConnectorFactory connectorFactory = JdbcFlinkConnectorFactory.INSTANCE;

  public JDBCEngine(@NonNull SqrlConfig connectorConfig) {
    super(JDBCEngineFactory.ENGINE_NAME, Type.DATABASE, STANDARD_DATABASE);
//        CAPABILITIES_BY_DIALECT.get(configuration.getDialect()));
    this.connectorConfig = connectorConfig;
    this.connector = JdbcDataSystemConnector.fromFlinkConnector(connectorConfig);
  }

  @Override
  public boolean supports(FunctionDefinition function) {
    return FunctionUtil.getTimestampPreservingFunction(function)
        .isEmpty(); //TODO: @Daniel: change to determining which functions are supported by dialect & database type
  }


  @Override
  public TableConfig getSinkConfig(String sinkName) {
    TableConfig.Builder tblBuilder = TableConfig.builder(sinkName)
        .setType(ExternalDataType.sink);
    tblBuilder.copyConnectorConfig(connectorFactory.fromBaseConfig(connectorConfig, sinkName));
    return tblBuilder.build();
  }

  @Override
  public IndexSelectorConfig getIndexSelectorConfig() {
    return IndexSelectorConfigByDialect.of(connector.getDialect());
  }

  @Override
  public EnginePhysicalPlan plan(StagePlan plan,
      List<StageSink> inputs, ExecutionPipeline pipeline, SqrlFramework framework,
      ErrorCollector errorCollector) {
    Preconditions.checkArgument(plan instanceof DatabaseStagePlan);
    DatabaseStagePlan dbPlan = (DatabaseStagePlan) plan;
    JdbcDDLFactory factory =
        (new JdbcDDLServiceLoader()).load(connector.getDialect())
            .orElseThrow(() -> new RuntimeException("Could not find DDL factory"));

    List<SqlDDLStatement> ddlStatements = StreamUtil.filterByClass(inputs,
            EngineSink.class)
        .map(factory::createTable)
        .collect(Collectors.toList());

    List<SqlDDLStatement> typeExtensions = extractTypeExtensions(dbPlan.getQueries());

    ddlStatements = ListUtils.union(typeExtensions, ddlStatements);

    dbPlan.getIndexDefinitions().stream()
            .map(factory::createIndex)
            .forEach(ddlStatements::add);

    Map<IdentifiedQuery, QueryTemplate> databaseQueries = dbPlan.getQueries().stream()
        .collect(Collectors.toMap(ReadQuery::getQuery, q -> new QueryTemplate(q.getRelNode())));

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

  @Override
  public boolean supportsType(java.lang.reflect.Type type) {
    JdbcTypeSerializer jdbcTypeSerializer = ServiceLoaderDiscovery.get(JdbcTypeSerializer.class,
        (Function<JdbcTypeSerializer, String>) JdbcTypeSerializer::getDialectId,
        connector.getDialect().getId(),
        (Function<JdbcTypeSerializer, String>) jdbcTypeSerializer1 -> jdbcTypeSerializer1.getConversionClass().getTypeName(),
        type.getTypeName());

    if (jdbcTypeSerializer != null) {
      return true;
    }

    return super.supportsType(type);
  }
}