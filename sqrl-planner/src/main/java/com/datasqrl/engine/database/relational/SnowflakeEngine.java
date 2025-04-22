package com.datasqrl.engine.database.relational;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.convert.SnowflakeSqlNodeToString;
import com.datasqrl.calcite.dialect.snowflake.SqlCreateIcebergTableFromObjectStorage;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EmptyEngineConfig;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.datatype.DataTypeMapper;
import com.datasqrl.datatype.snowflake.SnowflakeIcebergDataTypeMapper;
import com.datasqrl.engine.database.DatabasePhysicalPlanOld;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.jdbc.DatabaseType;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.sql.SqlDDLStatement;
import com.datasqrl.util.StreamUtil;
import com.google.inject.Inject;

import lombok.NonNull;

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
  protected DatabaseType getDatabaseType() {
    return DatabaseType.SNOWFLAKE;
  }

  @Override
  public JdbcStatementFactory getStatementFactory() {
    return new SnowflakeStatementFactory(engineConfig);
  }

}
