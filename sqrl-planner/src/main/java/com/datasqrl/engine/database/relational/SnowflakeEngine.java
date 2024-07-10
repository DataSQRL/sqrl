package com.datasqrl.engine.database.relational;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.convert.SnowflakeSqlNodeToString;
import com.datasqrl.calcite.dialect.ExtendedSnowflakeSqlDialect;
import com.datasqrl.calcite.dialect.snowflake.SqlCreateIcebergTableFromObjectStorage;
import com.datasqrl.calcite.dialect.snowflake.SqlCreateSnowflakeView;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.EngineFactory;
import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EmptyEngineConfig;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.global.PhysicalDAGPlan.DatabaseStagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.ReadQuery;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.sql.SqlDDLStatement;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.table.functions.FunctionDefinition;

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
  public EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs, ExecutionPipeline pipeline,
      SqrlFramework framework, ErrorCollector errorCollector) {

    List<SqlDDLStatement> ddlStatements = new ArrayList<>();

    EngineConfig engineConfig = connectorConfig;

    for (EngineSink sink : StreamUtil.filterByClass(inputs, EngineSink.class).collect(Collectors.toList())) {
      SqlLiteral externalVolume = SqlLiteral.createCharString(
          (String)engineConfig.toMap().get("external-volume"), SqlParserPos.ZERO);

      SqlCreateIcebergTableFromObjectStorage icebergTable = new SqlCreateIcebergTableFromObjectStorage(SqlParserPos.ZERO,
          false, false,
          new SqlIdentifier(sink.getNameId(), SqlParserPos.ZERO),
          externalVolume,
          SqlLiteral.createCharString((String)engineConfig.toMap().get("catalog-name"),
              SqlParserPos.ZERO), null,
          null, null, null);
      ddlStatements.add(()->icebergTable.toSqlString(CalciteSqlDialect.DEFAULT).getSql());
    }

    Preconditions.checkArgument(plan instanceof DatabaseStagePlan);
    DatabaseStagePlan dbPlan = (DatabaseStagePlan) plan;

    Map<IdentifiedQuery, QueryTemplate> databaseQueries = dbPlan.getQueries().stream()
        .collect(Collectors.toMap(ReadQuery::getQuery, q -> new QueryTemplate(q.getRelNode())));

    List<Map<String, String>> queries = new ArrayList<>();

    for (Map.Entry<IdentifiedQuery, QueryTemplate> entry : databaseQueries.entrySet()) {
      RelNode relNode = entry.getValue().getRelNode();
      SqlParserPos pos = new SqlParserPos(0, 0);
      SqlIdentifier viewName = new SqlIdentifier(entry.getKey().getNameId(), pos);
      SqlNodeList columnList = new SqlNodeList(relNode.getRowType().getFieldList().stream()
              .map(f->new SqlIdentifier(f.getName(), SqlParserPos.ZERO))
          .collect(Collectors.toList()), pos);

      SqlCharStringLiteral comment = SqlLiteral.createCharString("", pos);

      SqlSelect select =(SqlSelect) framework.getQueryPlanner()
          .relToSql(Dialect.SNOWFLAKE, relNode).getSqlNode();

      SqlCreateSnowflakeView createView = new SqlCreateSnowflakeView(pos, true, true, false, false, null, viewName, columnList,
          select, comment, false);

      SnowflakeSqlNodeToString toString = new SnowflakeSqlNodeToString();
      String sql = toString.convert(() -> createView).getSql();

      queries.add(Map.of("sql", sql));
    }


    return new SnowflakePlan(ddlStatements, queries);
  }

  @Override
  public boolean supports(FunctionDefinition function) {
    return true;
  }

  @Value
  public static class SnowflakePlan implements EnginePhysicalPlan {

    List<SqlDDLStatement> ddl;
    List<Map<String, String>> queries;

  }

  @Override
  public @NonNull EngineFactory.Type getType() {
    return Type.QUERY;
  }
}
