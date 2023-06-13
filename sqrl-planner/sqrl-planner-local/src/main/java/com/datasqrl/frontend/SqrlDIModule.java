package com.datasqrl.frontend;

import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.functions.FlinkBackedFunctionCatalog;
import com.datasqrl.functions.SqrlFunctionCatalog;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.parse.SqrlParser;
import com.datasqrl.parse.SqrlParserImpl;
import com.datasqrl.plan.local.generate.DebuggerConfig;
import com.datasqrl.schema.TypeFactory;
import com.google.inject.AbstractModule;
import org.apache.calcite.rel.type.RelDataTypeFactory;

public class SqrlDIModule extends AbstractModule {

  private final ExecutionPipeline pipeline;
  private final DebuggerConfig debugConfig;
  private final ModuleLoader moduleLoader;
  private final ErrorSink errorSink;
  private final ErrorCollector errors;

  public SqrlDIModule(
      ExecutionPipeline pipeline,
      DebuggerConfig debugConfig,
      ModuleLoader moduleLoader,
      ErrorSink errorSink,
      ErrorCollector errors) {
    this.pipeline = pipeline;
    this.debugConfig = debugConfig;
    this.moduleLoader = moduleLoader;
    this.errorSink = errorSink;
    this.errors = errors;
  }

  @Override
  protected void configure() {
    bind(RelDataTypeFactory.class).toInstance(TypeFactory.getTypeFactory());
    bind(ModuleLoader.class).toInstance(moduleLoader);
    bind(ErrorCollector.class).toInstance(errors);
    bind(NameCanonicalizer.class).toInstance(NameCanonicalizer.SYSTEM);
    bind(SqrlFunctionCatalog.class).toInstance(new FlinkBackedFunctionCatalog());
    bind(SqrlParser.class).toInstance(new SqrlParserImpl());
    bind(ExecutionPipeline.class).toInstance(pipeline);
    bind(DebuggerConfig.class).toInstance(debugConfig);
    bind(ErrorSink.class).toInstance(errorSink);
  }
}