package com.datasqrl.engine.database.relational;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.config.ConnectorFactoryContext;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EmptyEngineConfig;
import com.datasqrl.config.TableConfig;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.google.inject.Inject;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import lombok.NonNull;

public class IcebergEngine extends AbstractJDBCEngine {

  @Inject
  public IcebergEngine(
      @NonNull PackageJson json,
      ConnectorFactoryFactory connectorFactory) {
    super(IcebergEngineFactory.ENGINE_NAME,
        json.getEngines().getEngineConfig(IcebergEngineFactory.ENGINE_NAME)
            .orElseGet(()-> new EmptyEngineConfig(IcebergEngineFactory.ENGINE_NAME)),
        connectorFactory);
  }

  @Override
  public EnginePhysicalPlan plan(StagePlan plan, List<StagePlan> stagePlans, List<StageSink> inputs, ExecutionPipeline pipeline,
      SqrlFramework framework, ErrorCollector errorCollector) {

    return new IcebergPlan();
  }

  public static class IcebergPlan implements EnginePhysicalPlan, Serializable {
    public Boolean noProps = true;

    public Boolean getNoProps() {
      return noProps;
    }
  }

  @Override
  public TableConfig getSinkConfig(String tableName) {
    TableConfig sourceAndSink = connectorFactory
        .create(null, IcebergEngineFactory.ENGINE_NAME).get().createSourceAndSink(
            new ConnectorFactoryContext(tableName, Map.of("table-name", tableName)));
    return sourceAndSink;
  }
}
