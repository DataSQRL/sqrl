/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational;

import static com.datasqrl.engine.EngineFeature.STANDARD_DATABASE;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.ConnectorConf;
import com.datasqrl.config.ConnectorFactoryContext;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.config.TableConfig;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.engine.database.relational.ddl.JdbcDDLFactory;
import com.datasqrl.engine.database.relational.ddl.JdbcDDLServiceLoader;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.function.DowncastFunction;
import com.datasqrl.functions.json.RowToJsonDowncastFunction;
import com.datasqrl.plan.global.IndexSelectorConfig;
import com.datasqrl.plan.global.PhysicalDAGPlan.DatabaseStagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.ReadQuery;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.sql.PgExtension;
import com.datasqrl.sql.SqlDDLStatement;
import com.datasqrl.type.JdbcTypeSerializer;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.FunctionUtil;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.commons.collections.ListUtils;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

@Slf4j
public class AbstractJDBCEngine extends ExecutionEngine.Base implements DatabaseEngine {

//  public static final EnumMap<Dialect, EnumSet<EngineCapability>> CAPABILITIES_BY_DIALECT = new EnumMap<Dialect, EnumSet<EngineCapability>>(
//      Dialect.class);

//  static {
//    CAPABILITIES_BY_DIALECT.put(Dialect.POSTGRES, EnumSet.of(NOW, GLOBAL_SORT, MULTI_RANK));
//    CAPABILITIES_BY_DIALECT.put(Dialect.H2, EnumSet.of(NOW, GLOBAL_SORT, MULTI_RANK));
//  }

  @Getter
  final EngineConfig connectorConfig;

  private final ConnectorFactoryFactory connectorFactory;

  public AbstractJDBCEngine(@NonNull EngineConfig connectorConfig, ConnectorFactoryFactory connectorFactory) {
    super(PostgresEngineFactory.ENGINE_NAME, Type.DATABASE, STANDARD_DATABASE);
    this.connectorConfig = connectorConfig;
    this.connectorFactory = connectorFactory;
  }

  @Override
  public boolean supports(FunctionDefinition function) {
    //TODO: @Daniel: change to determining which functions are supported by dialect & database type
    //This is a hack - we just check that it's not a tumble window function
    return FunctionUtil.getSqrlTimeTumbleFunction(function).isEmpty();
  }


  @Override
  public TableConfig getSinkConfig(String tableName) {
    return connectorFactory
        .create(Type.DATABASE, getDialect().getId()).get().createSourceAndSink(
            new ConnectorFactoryContext(tableName, Map.of("table-name", tableName)));
  }

  @Override
  public IndexSelectorConfig getIndexSelectorConfig() {
    return IndexSelectorConfigByDialect.of(getDialect());
  }

  private JdbcDialect getDialect() {
    return JdbcDialect.Postgres;
  }

  @Override
  public EnginePhysicalPlan plan(StagePlan plan,
      List<StageSink> inputs, ExecutionPipeline pipeline, SqrlFramework framework,
      ErrorCollector errorCollector) {
    Preconditions.checkArgument(plan instanceof DatabaseStagePlan);
    DatabaseStagePlan dbPlan = (DatabaseStagePlan) plan;
    JdbcDDLFactory factory =
        (new JdbcDDLServiceLoader()).load(getDialect())
            .orElseThrow(() -> new RuntimeException("Could not find DDL factory"));

    List<SqlDDLStatement> ddlStatements = StreamUtil.filterByClass(inputs,
            EngineSink.class)
        .map(factory::createTable)
        .collect(Collectors.toList());

    List<SqlDDLStatement> typeExtensions = extractTypeExtensions(dbPlan.getQueries());

    ddlStatements = ListUtils.union(typeExtensions, ddlStatements);

    dbPlan.getIndexDefinitions().stream().sorted()
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
  public Optional<DowncastFunction> getDowncastFunction(RelDataType type) {
    if (type instanceof RawRelDataType) {
      Class<?> defaultConversion = ((RawRelDataType) type).getRawType().getDefaultConversion();

      JdbcTypeSerializer jdbcTypeSerializer = ServiceLoaderDiscovery.get(JdbcTypeSerializer.class,
          (Function<JdbcTypeSerializer, String>) JdbcTypeSerializer::getDialectId,
          getDialect().getId(),
          (Function<JdbcTypeSerializer, String>) jdbcTypeSerializer1 -> jdbcTypeSerializer1.getConversionClass()
              .getTypeName(),
          defaultConversion.getTypeName());

      if (jdbcTypeSerializer != null) {
        return Optional.empty();
      } else {
        return Optional.of(new RowToJsonDowncastFunction()); //try to downcast any raw to json
      }
    } else if (type instanceof RelRecordType) { //type is array of rows or rows
      return Optional.of(new RowToJsonDowncastFunction());
    } else if (type.getComponentType() != null && !isScalar(type.getComponentType())) { //allow primitive 1d arrays
      return Optional.of(new RowToJsonDowncastFunction());
    }

    return super.getDowncastFunction(type);
  }

  private boolean isScalar(RelDataType componentType) {
    switch (componentType.getSqlTypeName()) {
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case CHAR:
      case VARCHAR:
      case BINARY:
      case VARBINARY:
      case DATE:
      case TIMESTAMP:
      case TIME_WITH_LOCAL_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case DECIMAL:
        return true;
      default:
        return false;
    }
  }

  public ConnectorConf getConnectorFactoryConfig() {
    return connectorFactory.getConfig(getDialect().getId());
  }
}