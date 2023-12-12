package com.datasqrl.plan.local.analyze;

import com.datasqrl.TestModuleFactory;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.APIConnectorManagerImpl;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.google.inject.Inject;
import java.util.Optional;
import lombok.Value;
import lombok.experimental.Delegate;

@Value
public class MockAPIConnectorManager implements APIConnectorManager {

  @Delegate
  APIConnectorManagerImpl apiConnectorManager;

  @Inject
  public MockAPIConnectorManager(SqrlFramework framework, ExecutionPipeline pipeline) {
    ErrorCollector errors = ErrorCollector.root();

    apiConnectorManager  = new APIConnectorManagerImpl(
        new CalciteTableFactory(framework),
        pipeline,
        errors,
        new MockModuleLoader(null, TestModuleFactory.createRetail(framework), Optional.empty()),
        new TypeFactory()
    );
  }
}
