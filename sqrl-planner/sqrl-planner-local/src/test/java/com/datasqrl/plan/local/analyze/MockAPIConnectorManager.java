package com.datasqrl.plan.local.analyze;

import com.datasqrl.TestModuleFactory;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.config.LogEngineSupplier;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.APIConnectorManagerImpl;
import com.datasqrl.plan.table.CalciteTableFactory;
import java.util.Optional;
import lombok.Value;
import lombok.experimental.Delegate;
import org.apache.calcite.jdbc.SqrlSchema;

@Value
public class MockAPIConnectorManager implements APIConnectorManager {

  @Delegate
  APIConnectorManagerImpl apiConnectorManager;

  public MockAPIConnectorManager(SqrlFramework framework, ExecutionPipeline pipeline,
      ErrorCollector errors) {
    CalciteTableFactory calciteTableFactory = new CalciteTableFactory(framework);
    apiConnectorManager  = new APIConnectorManagerImpl(
        calciteTableFactory,
        new LogEngineSupplier(pipeline),
        errors,
        new MockModuleLoader(null, TestModuleFactory.createRetail(calciteTableFactory), Optional.empty()),
        new TypeFactory(),new SqrlSchema(new TypeFactory(), NameCanonicalizer.SYSTEM)
    );
  }
}
