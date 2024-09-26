package com.datasqrl.engine.database.relational;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.convert.SnowflakeSqlNodeToString;
import com.datasqrl.calcite.dialect.snowflake.SqlCreateIcebergTableFromObjectStorage;
import com.datasqrl.calcite.dialect.snowflake.SqlCreateSnowflakeView;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.EngineFactory;
import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EmptyEngineConfig;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.datatype.snowflake.SnowflakeIcebergDataTypeMapper;
import com.datasqrl.engine.database.DatabasePhysicalPlan;
import com.datasqrl.engine.database.DatabaseViewPhysicalPlan;
import com.datasqrl.engine.database.DatabaseViewPhysicalPlan.DatabaseView;
import com.datasqrl.engine.database.DatabaseViewPhysicalPlan.DatabaseViewImpl;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.stream.flink.connector.CastFunction;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.global.PhysicalDAGPlan.DatabaseStagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.ReadQuery;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.sql.SqlDDLStatement;
import com.datasqrl.util.StreamUtil;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.RelBuilder;

public class SnowflakeEngine extends AbstractJDBCQueryEngine {

  @Inject
  public SnowflakeEngine(
      @NonNull PackageJson json,
      ConnectorFactoryFactory connectorFactory) {
    super(SnowflakeEngineFactory.ENGINE_NAME,
        json.getEngines().getEngineConfig(SnowflakeEngineFactory.ENGINE_NAME)
            .orElseGet(()-> new EmptyEngineConfig(SnowflakeEngineFactory.ENGINE_NAME)),
        connectorFactory);
  }

  @Override
  protected JdbcDialect getDialect() {
    return JdbcDialect.Snowflake;
  }

  @Override
  public DatabasePhysicalPlan plan(ConnectorFactoryFactory tableConnectorFactory, EngineConfig tableConnectorConfig,
      StagePlan plan, List<StageSink> inputs, ExecutionPipeline pipeline,
      List<StagePlan> stagePlans, SqrlFramework framework, ErrorCollector errorCollector) {

    List<SqlDDLStatement> ddlStatements = new ArrayList<>();

    EngineConfig engineConfig = connectorConfig;

    for (EngineSink sink : StreamUtil.filterByClass(inputs, EngineSink.class).collect(Collectors.toList())) {
      SqlLiteral externalVolume = SqlLiteral.createCharString(
          (String)engineConfig.toMap().get("external-volume"), SqlParserPos.ZERO);

      SqlCreateIcebergTableFromObjectStorage icebergTable = new SqlCreateIcebergTableFromObjectStorage(SqlParserPos.ZERO,
          true, false,
          new SqlIdentifier(sink.getNameId(), SqlParserPos.ZERO),
          externalVolume,
          SqlLiteral.createCharString((String)engineConfig.toMap().get("catalog-name"),
              SqlParserPos.ZERO),
          SqlLiteral.createCharString(sink.getNameId(), SqlParserPos.ZERO),
          null,
          null, null, null);

      SnowflakeSqlNodeToString toString = new SnowflakeSqlNodeToString();
      String sql = toString.convert(() -> icebergTable).getSql();
      ddlStatements.add(()->sql);
    }

    Preconditions.checkArgument(plan instanceof DatabaseStagePlan);
    DatabaseStagePlan dbPlan = (DatabaseStagePlan) plan;

    Map<IdentifiedQuery, QueryTemplate> databaseQueries = dbPlan.getQueries().stream()
        .collect(Collectors.toMap(ReadQuery::getQuery, q -> new QueryTemplate("snowflake", q.getRelNode())));

    List<DatabaseView> views = new ArrayList<>();

    SnowflakeIcebergDataTypeMapper icebergDataTypeMapper = new SnowflakeIcebergDataTypeMapper();
    for (Map.Entry<IdentifiedQuery, QueryTemplate> entry : databaseQueries.entrySet()) {
      RelNode relNode = entry.getValue().getRelNode();
      relNode = relNode.accept(new RelShuttleImpl(){
        @Override
        public RelNode visit(TableScan scan) {
          return applyUpcasting(framework.getQueryPlanner().getRelBuilder(),
              scan, icebergDataTypeMapper);
        }
      });
      if (hasDynamicParam(relNode)) {
        continue;
      }

      SqlParserPos pos = new SqlParserPos(0, 0);
      String viewName = getViewName(entry.getKey());
      SqlIdentifier viewNameIdentifier = new SqlIdentifier(viewName, pos);
      SqlNodeList columnList = new SqlNodeList(relNode.getRowType().getFieldList().stream()
              .map(f->new SqlIdentifier(f.getName(), SqlParserPos.ZERO))
          .collect(Collectors.toList()), pos);

      SqlNode select = framework.getQueryPlanner()
          .relToSql(Dialect.SNOWFLAKE, relNode).getSqlNode();

      SqlCreateSnowflakeView createView = new SqlCreateSnowflakeView(pos, true, false, false, false, null, viewNameIdentifier, columnList,
          select, null, false);

      SnowflakeSqlNodeToString toString = new SnowflakeSqlNodeToString();
      String sql = toString.convert(() -> createView).getSql();

      views.add(new DatabaseViewImpl(viewName, sql));
    }


    return new SnowflakePlan(ddlStatements, views, databaseQueries);
  }

  private RelNode applyUpcasting(RelBuilder relBuilder, RelNode relNode,
      SnowflakeIcebergDataTypeMapper icebergDataTypeMapper) {
    //Apply upcasting if reading a json/other function directly from the table.
    relBuilder.push(relNode);

    AtomicBoolean hasChanged = new AtomicBoolean();
    List<RexNode> fields = relNode.getRowType().getFieldList().stream()
        .map(field -> convertField(field, hasChanged, relBuilder, icebergDataTypeMapper))
        .collect(Collectors.toList());

    if (hasChanged.get()) {
      return relBuilder.project(fields, relNode.getRowType().getFieldNames(), true).build();
    }
    return relNode;
  }


  private RexNode convertField(RelDataTypeField field, AtomicBoolean hasChanged, RelBuilder relBuilder,
      SnowflakeIcebergDataTypeMapper icebergDataTypeMapper) {
    RelDataType type = field.getType();
    if (icebergDataTypeMapper.nativeTypeSupport(type)) {
      return relBuilder.field(field.getIndex());
    }

    Optional<CastFunction> castFunction = icebergDataTypeMapper.convertType(type);
    if (castFunction.isEmpty()) {
      throw new RuntimeException("Could not find upcast function for: " + type.getFullTypeString());
    }

    CastFunction castFunction1 = castFunction.get();

    hasChanged.set(true);
    return relBuilder.getRexBuilder()
        .makeCall(castFunction1.getFunction(), List.of(relBuilder.field(field.getIndex())));
  }

//  @Override
//  public boolean supports(FunctionDefinition function) {
//    return true;
//  }

  @Value
  public static class SnowflakePlan implements DatabaseViewPhysicalPlan {

    List<SqlDDLStatement> ddl;
    List<DatabaseView> views;
    @JsonIgnore
    Map<IdentifiedQuery, QueryTemplate> queryPlans;
  }

  @Override
  public @NonNull EngineFactory.Type getType() {
    return Type.QUERY;
  }
}
