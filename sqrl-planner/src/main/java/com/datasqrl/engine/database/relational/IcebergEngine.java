package com.datasqrl.engine.database.relational;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EmptyEngineConfig;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.database.QueryEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.global.PhysicalDAGPlan.DatabaseStagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.util.StreamUtil;
import com.google.inject.Inject;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;

public class IcebergEngine extends AbstractJDBCTableFormatEngine {

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
  public boolean supportsQueryEngine(QueryEngine engine) {
    return engine instanceof SnowflakeEngine;
  }

  @Override
  protected JdbcDialect getDialect() {
    return JdbcDialect.Iceberg;
  }

  @Override
  public EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs, ExecutionPipeline pipeline,
      SqrlFramework framework, ErrorCollector errorCollector) {

    EnginePhysicalPlan enginePlan = queryEngines.values().stream().findFirst().get()
        .plan(plan, inputs, pipeline, framework, errorCollector);
    DatabaseStagePlan dbPlan = ( DatabaseStagePlan) plan;

    StreamUtil.filterByClass(inputs,
        EngineSink.class).forEach(s -> {
          String tableId = s.getNameId();
          Optional<IndexDefinition> optIndex =  dbPlan.getIndexDefinitions().stream().filter(i -> i.getTableId().equals(tableId)).findFirst();
          if (optIndex.isPresent()) {
            IndexDefinition mainIndex = optIndex.get();
//            System.out.println("Table: " + tableId);
//            System.out.println("Partition columns: " + String.join(", ",  mainIndex.getColumnNames().subList(0, mainIndex.getPartitionOffset())));
//            System.out.println("Sort columns: " + String.join(", ",  mainIndex.getColumnNames().subList(mainIndex.getPartitionOffset(), mainIndex.getColumns().size())));
          } else {
//            System.out.println("No partition on table: " + tableId);
          }
    });


    //nothing needs to be created for aws-glue
    return new IcebergPlan(enginePlan);
  }
}
