package com.datasqrl.frontend;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.parse.SqrlParser;
import com.datasqrl.parse.SqrlParserImpl;
import com.datasqrl.plan.local.generate.DebuggerConfig;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.plan.table.TableConverter;
import com.datasqrl.plan.table.TableIdFactory;
import com.google.inject.AbstractModule;
import org.apache.calcite.rel.type.RelDataTypeFactory;

public class SqrlDIModule extends AbstractModule {

  protected final ExecutionPipeline pipeline;
  protected final DebuggerConfig debugConfig;
  protected final ModuleLoader moduleLoader;
  protected final ErrorSink errorSink;
  protected final ErrorCollector errors;
  protected final SqrlFramework sqrlFramework;
  private final NameCanonicalizer nameCanonicalizer;

  public SqrlDIModule(
      ExecutionPipeline pipeline,
      DebuggerConfig debugConfig,
      ModuleLoader moduleLoader,
      ErrorSink errorSink,
      ErrorCollector errors,
      SqrlFramework sqrlFramework,
      NameCanonicalizer nameCanonicalizer) {
    this.pipeline = pipeline;
    this.debugConfig = debugConfig;
    this.moduleLoader = moduleLoader;
    this.errorSink = errorSink;
    this.errors = errors;
    this.sqrlFramework = sqrlFramework;
    this.nameCanonicalizer = nameCanonicalizer;
  }

  @Override
  protected void configure() {
    bind(SqrlFramework.class).toInstance(sqrlFramework);
    bind(CalciteTableFactory.class).toInstance(new CalciteTableFactory(sqrlFramework));
    bind(RelDataTypeFactory.class).toInstance(TypeFactory.getTypeFactory());
    bind(ModuleLoader.class).toInstance(moduleLoader);
    bind(ErrorCollector.class).toInstance(errors);
    bind(NameCanonicalizer.class).toInstance(nameCanonicalizer);
    bind(SqrlParser.class).toInstance(new SqrlParserImpl());
    bind(ExecutionPipeline.class).toInstance(pipeline);
    bind(DebuggerConfig.class).toInstance(debugConfig);
    bind(ErrorSink.class).toInstance(errorSink);
  }
}