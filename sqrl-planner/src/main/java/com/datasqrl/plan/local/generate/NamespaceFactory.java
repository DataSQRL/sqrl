package com.datasqrl.plan.local.generate;

import com.datasqrl.functions.SqrlFunctionCatalog;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.calcite.table.CalciteTableFactory;
import com.google.inject.Inject;
import org.apache.calcite.jdbc.SqrlSchema;

/**
 * Help with creation
 */
public class NamespaceFactory {

  CalciteTableFactory calciteTableFactory;
  SqrlFunctionCatalog functionCatalog;
  ExecutionPipeline pipeline;
  SqrlSchema schema;
  ErrorCollector errors;

  @Inject
  public NamespaceFactory(CalciteTableFactory calciteTableFactory,
      SqrlFunctionCatalog functionCatalog, ExecutionPipeline pipeline, SqrlSchema schema,
      ErrorCollector errors) {
    this.calciteTableFactory = calciteTableFactory;
    this.functionCatalog = functionCatalog;
    this.pipeline = pipeline;
    this.schema = schema;
    this.errors = errors;
  }

  public Namespace createNamespace() {
    return new Namespace(calciteTableFactory, functionCatalog, pipeline, schema);
  }
}
