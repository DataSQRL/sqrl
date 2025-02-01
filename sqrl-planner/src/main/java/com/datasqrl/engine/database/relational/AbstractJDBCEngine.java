/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.EngineType;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.datatype.DataTypeMapper;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.database.DatabasePhysicalPlan;
import com.datasqrl.engine.database.DatabaseViewPhysicalPlan.DatabaseView;
import com.datasqrl.engine.database.DatabaseViewPhysicalPlan.DatabaseViewImpl;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.engine.database.relational.ddl.JdbcDDLFactory;
import com.datasqrl.engine.database.relational.ddl.JdbcDDLServiceLoader;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.stream.flink.connector.CastFunction;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

/**
 * This is an abstract engine implementation that provides shared functionalities for relational/jdbc-compatible
 * engines.
 *
 * It implements the physical plan construction by creating DDL statements and queries for the relational database dialect.
 */
@Slf4j
public abstract class AbstractJDBCEngine extends ExecutionEngine.Base implements ExecutionEngine {

  public AbstractJDBCEngine(@NonNull String name, @NonNull EngineType type,
      @NonNull EnumSet<EngineFeature> capabilities) {
    super(name, type, capabilities);
  }

  protected abstract JdbcDialect getDialect();

  @Override
  public DatabasePhysicalPlan plan(StagePlan plan, List<StageSink> inputs,
      ExecutionPipeline pipeline, List<StagePlan> stagePlans, SqrlFramework framework, ErrorCollector errorCollector) {

    Preconditions.checkArgument(plan instanceof DatabaseStagePlan);
    DatabaseStagePlan dbPlan = (DatabaseStagePlan) plan;
    Optional<JdbcDDLFactory> optFactory =
        (new JdbcDDLServiceLoader()).load(getDialect());
    List<SqlDDLStatement> ddlStatements = new ArrayList<>();

    //Create DDL statements if factory is found
    if (optFactory.isPresent()) {
      JdbcDDLFactory factory = optFactory.get();
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
    }


    Map<IdentifiedQuery, QueryTemplate> databaseQueries = dbPlan.getQueries().stream()
        .collect(Collectors.toMap(ReadQuery::getQuery, q -> new QueryTemplate(
            getDialect().name().toLowerCase(), q.getRelNode())));

    List<DatabaseView> views = new ArrayList<>();
    Optional<DataTypeMapper> upCastingMapper = getUpCastingMapper();
    //Create database views
    for (Map.Entry<IdentifiedQuery, QueryTemplate> entry : databaseQueries.entrySet()) {
      Optional<Name> optViewName = entry.getKey().getViewName();
      if (optViewName.isEmpty()) continue;
      RelNode relNode = entry.getValue().getRelNode();
      if (upCastingMapper.isPresent()) {
        relNode = relNode.accept(new RelShuttleImpl(){
          @Override
          public RelNode visit(TableScan scan) {
            return applyUpcasting(framework, framework.getQueryPlanner().getRelBuilder(),
                scan, upCastingMapper.get());
          }
        });
      }
      SqlParserPos pos = new SqlParserPos(0, 0);
      String viewName = optViewName.get().getDisplay();
      SqlIdentifier viewNameIdentifier = new SqlIdentifier(viewName, pos);
      SqlNodeList columnList = new SqlNodeList(relNode.getRowType().getFieldList().stream()
          .map(f->new SqlIdentifier(f.getName(), SqlParserPos.ZERO))
          .collect(Collectors.toList()), pos);

      SqlNode sqlNode = framework.getQueryPlanner()
          .relToSql(Dialect.POSTGRES, relNode).getSqlNode();

      String viewSql = createView(viewNameIdentifier, pos, columnList, sqlNode);
      views.add(new DatabaseViewImpl(viewName, viewSql));
    }

    return new JDBCPhysicalPlan(ddlStatements,
        views,
        databaseQueries);
  }

  abstract protected String createView(SqlIdentifier viewNameIdentifier, SqlParserPos pos, SqlNodeList columnList, SqlNode viewSqlNode);

  /**
   * If writing to the database required downcasting of types, this should return the corresponding upcasting mapper
   * to restore the original type.
   * @return
   */
  protected Optional<DataTypeMapper> getUpCastingMapper() {
    return Optional.empty();
  }

  private RelNode applyUpcasting(SqrlFramework framework, RelBuilder relBuilder, RelNode relNode,
      DataTypeMapper dataTypeMapper) {
    //Apply upcasting if reading a json/other function directly from the table.
    relBuilder.push(relNode);

    AtomicBoolean hasChanged = new AtomicBoolean();
    List<RexNode> fields = relNode.getRowType().getFieldList().stream()
        .map(field -> convertField(framework, field, hasChanged, relBuilder, dataTypeMapper))
        .collect(Collectors.toList());

    if (hasChanged.get()) {
      return relBuilder.project(fields, relNode.getRowType().getFieldNames(), true).build();
    }
    return relNode;
  }


  private RexNode convertField(SqrlFramework framework, RelDataTypeField field, AtomicBoolean hasChanged, RelBuilder relBuilder,
      DataTypeMapper dataTypeMapper) {
    RelDataType type = field.getType();
    if (dataTypeMapper.nativeTypeSupport(type)) {
      return relBuilder.field(field.getIndex());
    }

    Optional<CastFunction> castFunction = dataTypeMapper.convertType(type);
    if (castFunction.isEmpty()) {
      throw new RuntimeException("Could not find upcast function for: " + type.getFullTypeString());
    }

    CastFunction castFunction1 = castFunction.get();

    hasChanged.set(true);

    framework.getFlinkFunctionCatalog().registerCatalogFunction(
        UnresolvedIdentifier.of(castFunction1.getFunction().getClass().getSimpleName()),
        castFunction1.getFunction().getClass(), true);

    hasChanged.set(true);

    List<SqlOperator> list = new ArrayList<>();
    framework.getSqrlOperatorTable()
        .lookupOperatorOverloads(new SqlIdentifier(castFunction1.getFunction().getClass().getSimpleName(), SqlParserPos.ZERO),
            SqlFunctionCategory.USER_DEFINED_FUNCTION,
            SqlSyntax.FUNCTION, list, SqlNameMatchers.liberal());

    return relBuilder.getRexBuilder()
        .makeCall(list.get(0), List.of(relBuilder.field(field.getIndex())));
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