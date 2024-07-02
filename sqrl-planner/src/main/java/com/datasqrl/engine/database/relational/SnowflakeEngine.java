package com.datasqrl.engine.database.relational;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EmptyEngineConfig;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.sql.SqlDDLStatement;
import com.google.inject.Inject;
import java.util.List;
import java.util.Map;
import lombok.NonNull;
import lombok.Value;

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

    JDBCPhysicalPlan snowflakePlan = (JDBCPhysicalPlan)super.plan(plan, inputs, pipeline, framework, errorCollector);

    return new SnowflakePlan(snowflakePlan.getDdl(), snowflakePlan.getQueries());
  }

  @Value
  public static class SnowflakePlan implements EnginePhysicalPlan {

    List<SqlDDLStatement> ddl;
    List<Map<String, String>> queries;

  }



}
